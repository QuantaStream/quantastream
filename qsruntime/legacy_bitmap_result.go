package qsruntime

import (
	"github.com/QuantaStream/quantastream/qsbridge"
	legacy "github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// LegacyBitmapQueryResultAdapter converts legacy bitmap-query responses to neutral results.
type LegacyBitmapQueryResultAdapter struct{}

// ToBitmapQueryResult converts a legacy shared.BitmapQueryResponse.
func (a LegacyBitmapQueryResultAdapter) ToBitmapQueryResult(response *legacy.BitmapQueryResponse) BitmapQueryResult {
	if response == nil {
		return BitmapQueryResult{
			Success:      false,
			ErrorMessage: "legacy bitmap query returned nil response",
		}
	}
	return BitmapQueryResult{
		Success:      response.Success,
		ErrorMessage: response.ErrorMessage,
		Count:        response.Count,
		Rownums:      bitmapRownums(response.Results),
	}
}

func bitmapRownums(bitmap *roaring64.Bitmap) []qsbridge.QuantaRownum {
	if bitmap == nil || bitmap.IsEmpty() {
		return nil
	}
	values := bitmap.ToArray()
	rownums := make([]qsbridge.QuantaRownum, len(values))
	for i, value := range values {
		rownums[i] = qsbridge.QuantaRownum(value)
	}
	return rownums
}
