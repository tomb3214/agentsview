package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCandidatePassagePreservesWholeEmbeddingWindow(t *testing.T) {
	content := strings.Repeat("é", 500) + " migration discussion"
	got := candidateChunkText(content, 0, 600)
	assert.Equal(t, content, got)
	assert.Greater(t, len([]rune(got)), 200)
	assert.Equal(t, 1, candidateChunkAt(content, 500*2, 300))
	assert.Equal(t, 0, candidateChunkAt(strings.Repeat("x", 900), 880, 1000))
}

func TestCandidatePassageKeepsBlankWindowIndexes(t *testing.T) {
	content := strings.Repeat(" ", 85) + "migration"
	assert.Empty(t, candidateChunkText(content, 0, 40))
	assert.Contains(t, candidateChunkText(content, 2, 40), "migration")
	assert.Empty(t, candidateChunkText(content, 99, 40))
	assert.Equal(t, -1, candidateChunkAt(content, len(content), 40))
}

func TestCandidatePassageMasksSecretAcrossChunkBoundary(t *testing.T) {
	secret := "ghp_" + "8Hk3Wn7Dz4Rp2Vx9Mb6Tj0Qc5Lm1Yp8Bv4Hg"
	content := strings.Repeat("x ", 20) + "token=" + secret + " retained context"
	got := candidateChunkText(content, 1, 60)
	require.NotEmpty(t, got)
	assert.NotContains(t, got, secret)
	assert.NotContains(t, got, "Wn7Dz4Rp2Vx9Mb6Tj0Qc5Lm1Yp8Bv")
	assert.Contains(t, got, "retained context")
}
