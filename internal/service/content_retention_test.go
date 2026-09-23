package service_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/service"
	"testing"
)

func TestDirectSearchRetainsBusinessEvidenceWithoutReveal(t *testing.T) {
	t.Parallel()
	d := dbtest.OpenTestDB(t)
	original := "campaign=Xa9Kd03Lm5Qp7Rt2Vw8Zb4Nc6 token=trip-code secret=walking-route"
	seedServiceSearchSession(t, d, "business-original", "proj", original)
	be := service.NewDirectBackend(d, nil)
	res, err := be.SearchContent(context.Background(), service.ContentSearchRequest{
		Pattern: "token=", Mode: "substring", Limit: 5,
	})
	require.NoError(t, err)
	require.Len(t, res.Matches, 1)
	assert.Equal(t, original, res.Matches[0].Snippet)
}
