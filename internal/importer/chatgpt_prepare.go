package importer

import (
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ChatGPTPreparationManifest is an explicit admission decision. Evidence is a
// reference to externally verified membership, not a claim inferred from titles.
type ChatGPTPreparationManifest struct {
	Version            int               `json:"version"`
	MembershipEvidence string            `json:"membership_evidence"`
	ConversationIDs    []string          `json:"conversation_ids"`
	Shards             []PreparationFile `json:"shards"`
}
type PreparationFile struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}
type ChatGPTPreparationReceipt struct {
	Version            int               `json:"version"`
	ManifestSHA256     string            `json:"manifest_sha256"`
	MembershipEvidence string            `json:"membership_evidence"`
	ConversationIDs    []string          `json:"conversation_ids"`
	Selected           int               `json:"selected"`
	Excluded           int               `json:"excluded"`
	Files              []PreparationFile `json:"files"`
	SearchScope        string            `json:"search_scope"`
}

func preparationHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// readPreparationFile rejects symlinks, including intermediate directories.
func readPreparationFile(root, name string) ([]byte, error) {
	if !filepath.IsLocal(name) {
		return nil, fmt.Errorf("non-local source path")
	}
	path := root
	for part := range strings.SplitSeq(name, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("symlink source rejected")
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("source is not a regular file")
	}
	return os.ReadFile(path)
}

// PrepareChatGPTExport performs no database or configuration access. Only after
// every input has been validated is an owner-private directory published. Raw
// selected objects retain all DAG branches; the existing importer still searches
// only current ancestry plus branches already archived by earlier imports.
func PrepareChatGPTExport(ctx context.Context, source, manifestPath, output string) (ChatGPTPreparationReceipt, error) {
	var receipt ChatGPTPreparationReceipt
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return receipt, err
	}
	var manifest ChatGPTPreparationManifest
	if err = json.Unmarshal(manifestBytes, &manifest, json.RejectUnknownMembers(true)); err != nil {
		return receipt, fmt.Errorf("invalid manifest: %w", err)
	}
	if manifest.Version != 1 || strings.TrimSpace(manifest.MembershipEvidence) == "" || len(manifest.ConversationIDs) == 0 || len(manifest.Shards) == 0 {
		return receipt, fmt.Errorf("manifest requires version 1, membership evidence, IDs and shards")
	}
	allowed := map[string]bool{}
	for _, id := range manifest.ConversationIDs {
		if id == "" || strings.TrimSpace(id) != id || allowed[id] {
			return receipt, fmt.Errorf("invalid or duplicate admitted ID")
		}
		allowed[id] = true
	}
	info, err := os.Lstat(source)
	if err != nil {
		return receipt, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return receipt, fmt.Errorf("source must be a real directory")
	}
	if _, err = os.Lstat(output); !os.IsNotExist(err) {
		return receipt, fmt.Errorf("output must not already exist")
	}
	shardHashes := map[string]string{}
	for _, f := range manifest.Shards {
		matched, _ := filepath.Match("conversations-*.json", f.Name)
		if filepath.Base(f.Name) != f.Name || (!matched && f.Name != "conversations.json") || len(f.SHA256) != 64 || shardHashes[f.Name] != "" {
			return receipt, fmt.Errorf("invalid or duplicate shard declaration")
		}
		shardHashes[f.Name] = f.SHA256
	}
	// Match the complete source inventory, including mixed single/sharded exports.
	entries, err := os.ReadDir(source)
	if err != nil {
		return receipt, err
	}
	found := 0
	for _, entry := range entries {
		matched, _ := filepath.Match("conversations-*.json", entry.Name())
		if matched || entry.Name() == "conversations.json" {
			if shardHashes[entry.Name()] == "" {
				return receipt, fmt.Errorf("undeclared conversation shard")
			}
			found++
		}
	}
	if found != len(shardHashes) {
		return receipt, fmt.Errorf("missing declared shard")
	}
	names := make([]string, 0, len(shardHashes))
	for name := range shardHashes {
		names = append(names, name)
	}
	sort.Strings(names)
	selected := map[string]jsontext.Value{}
	seen := map[string]bool{}
	pointers := map[string]bool{}
	for _, name := range names {
		if err = ctx.Err(); err != nil {
			return receipt, err
		}
		data, err := readPreparationFile(source, name)
		if err != nil {
			return receipt, err
		}
		if preparationHash(data) != shardHashes[name] {
			return receipt, fmt.Errorf("shard hash mismatch: %s", name)
		}
		var objects []jsontext.Value
		if err = json.Unmarshal(data, &objects); err != nil || strings.TrimSpace(string(data)) == "null" {
			return receipt, fmt.Errorf("invalid conversation array: %s", name)
		}
		for _, raw := range objects {
			var identity struct {
				ID             string                    `json:"id"`
				ConversationID string                    `json:"conversation_id"`
				Mapping        map[string]jsontext.Value `json:"mapping"`
			}
			if err = json.Unmarshal(raw, &identity); err != nil {
				return receipt, fmt.Errorf("invalid conversation object")
			}
			id := identity.ConversationID
			if id == "" {
				id = identity.ID
			}
			if id == "" || (identity.ID != "" && identity.ConversationID != "" && identity.ID != identity.ConversationID) || seen[id] {
				return receipt, fmt.Errorf("missing, conflicting or duplicate conversation identity")
			}
			seen[id] = true
			if !allowed[id] {
				receipt.Excluded++
				continue
			}
			if identity.Mapping == nil {
				return receipt, fmt.Errorf("admitted conversation lacks mapping")
			}
			selected[id] = append(jsontext.Value(nil), raw...)
			// Only explicit asset_pointer fields in admitted raw objects authorize assets.
			var value any
			if err = json.Unmarshal(raw, &value); err != nil {
				return receipt, err
			}
			collectPreparationPointers(value, pointers)
		}
	}
	if len(selected) != len(allowed) {
		return receipt, fmt.Errorf("admitted conversation missing from export")
	}
	ids := append([]string(nil), manifest.ConversationIDs...)
	sort.Strings(ids)
	// Keep each original object byte-for-byte, including mapping order and branches.
	var data []byte
	data = append(data, '[')
	for i, id := range ids {
		if i > 0 {
			data = append(data, ',')
		}
		data = append(data, selected[id]...)
	}
	data = append(data, ']', '\n')
	files := map[string][]byte{"conversations.json": data}
	index, err := preparationAssetIndex(source, pointers)
	if err != nil {
		return receipt, err
	}
	for pointer := range pointers {
		path, ok := index.Resolve(pointer)
		if !ok {
			return receipt, fmt.Errorf("admitted asset reference is unavailable")
		}
		name, err := filepath.Rel(source, path)
		if err != nil {
			return receipt, err
		}
		asset, err := readPreparationFile(source, name)
		if err != nil {
			return receipt, err
		}
		files[name] = asset
	}
	receipt.Version = 1
	receipt.ManifestSHA256 = preparationHash(manifestBytes)
	receipt.MembershipEvidence = manifest.MembershipEvidence
	receipt.ConversationIDs = ids
	receipt.Selected = len(ids)
	receipt.SearchScope = "current ancestry and previously archived branches; full raw mapping retained"
	names = names[:0]
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		receipt.Files = append(receipt.Files, PreparationFile{Name: name, SHA256: preparationHash(files[name])})
	}
	receiptBytes, err := json.Marshal(receipt, jsontext.WithIndent("  "))
	if err != nil {
		return receipt, err
	}
	files["preparation-receipt.json"] = append(receiptBytes, '\n')
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(output), ".chatgpt-preparation-")
	if err != nil {
		return receipt, err
	}
	defer os.RemoveAll(temporary)
	for name, content := range files {
		path := filepath.Join(temporary, name)
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return receipt, err
		}
		if err = os.WriteFile(path, content, 0600); err != nil {
			return receipt, err
		}
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	if _, err = os.Lstat(output); !os.IsNotExist(err) {
		return receipt, fmt.Errorf("output already exists")
	}
	if err = os.Rename(temporary, output); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func collectPreparationPointers(value any, pointers map[string]bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if key == "asset_pointer" {
				if pointer, ok := child.(string); ok && pointer != "" {
					pointers[pointer] = true
				}
			}
			collectPreparationPointers(child, pointers)
		}
	case []any:
		for _, child := range v {
			collectPreparationPointers(child, pointers)
		}
	}
}

// Do not let ambiguous export basenames select an unrelated asset silently.
func preparationAssetIndex(source string, pointers map[string]bool) (AssetIndex, error) {
	index := AssetIndex{entries: map[string]string{}}
	directories := []string{source}
	entries, err := os.ReadDir(source)
	if err != nil {
		return index, err
	}
	for _, entry := range entries {
		if entry.IsDir() && (entry.Name() == "dalle-generations" || strings.HasPrefix(entry.Name(), "user-")) {
			directories = append(directories, filepath.Join(source, entry.Name()))
		}
	}
	for _, directory := range directories {
		entries, err := os.ReadDir(directory)
		if err != nil {
			return index, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if directory == source && !strings.HasPrefix(entry.Name(), "file-") && !strings.HasPrefix(entry.Name(), "file_") {
				continue
			}
			prefix := extractAssetPrefix(entry.Name())
			if !pointers["file-service://"+prefix] && !pointers["sediment://"+prefix] {
				continue
			}
			if _, exists := index.entries[prefix]; exists {
				return index, fmt.Errorf("ambiguous export asset prefix")
			}
			index.entries[prefix] = filepath.Join(directory, entry.Name())
		}
	}
	return index, nil
}
