package shared

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

var bsiSignedBinaryMarker = []byte("QSBISIGN1")

// MarshalBSI preserves the BSI sign slice in addition to the value slices
// emitted by roaring64.BSI.MarshalBinary.
func MarshalBSI(bsi *roaring64.BSI) ([][]byte, error) {
	if bsi == nil {
		return nil, nil
	}
	data, err := bsi.MarshalBinary()
	if err != nil {
		return nil, err
	}
	sign := roaring64.NewBitmap()
	for _, columnID := range bsi.GetExistenceBitmap().ToArray() {
		if bsi.IsNegative(columnID) {
			sign.Add(columnID)
		}
	}
	signData, err := sign.MarshalBinary()
	if err != nil {
		return nil, err
	}
	data = append(data, signData)
	data = append(data, append([]byte(nil), bsiSignedBinaryMarker...))
	return data, nil
}

// UnmarshalBSI reads BSI data written by MarshalBSI. It remains compatible with
// older signless BSI payloads, though those older payloads cannot recover
// negative values once the sign slice has been dropped.
func UnmarshalBSI(bsi *roaring64.BSI, data [][]byte) error {
	if bsi == nil {
		return nil
	}
	if len(data) < 2 || !bytes.Equal(data[len(data)-1], bsiSignedBinaryMarker) {
		return unmarshalRoaringBSI(bsi, data)
	}
	sign := roaring64.NewBitmap()
	if err := UnmarshalBitmap(sign, data[len(data)-2]); err != nil {
		return err
	}
	payload := data[:len(data)-2]
	if sign.IsEmpty() {
		return unmarshalRoaringBSI(bsi, payload)
	}
	unsigned := roaring64.NewDefaultBSI()
	if err := unmarshalRoaringBSI(unsigned, payload); err != nil {
		return err
	}
	valueBitCount := len(payload) - 1
	offset := new(big.Int).Lsh(big.NewInt(1), uint(valueBitCount))
	rebuilt := roaring64.NewDefaultBSI()
	rows := unsigned.GetExistenceBitmap().ToArray()
	values := unsigned.GetBigValues(rows)
	for i, row := range rows {
		value := values[i]
		if value == nil {
			continue
		}
		copyValue := new(big.Int).Set(value)
		if sign.Contains(row) {
			copyValue.Sub(copyValue, offset)
		}
		rebuilt.SetBigValue(row, copyValue)
	}
	*bsi = *rebuilt
	return nil
}

func unmarshalRoaringBSI(bsi *roaring64.BSI, data [][]byte) (err error) {
	if len(data) == 0 {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("roaring BSI unmarshal failed: %v", recovered)
		}
	}()
	return bsi.UnmarshalBinary(data)
}

func describeBSIPayload(data [][]byte) string {
	marker := false
	if len(data) > 0 {
		marker = bytes.Equal(data[len(data)-1], bsiSignedBinaryMarker)
	}
	limit := len(data)
	if limit > 10 {
		limit = 10
	}
	summary := fmt.Sprintf("slices=%d marker=%t lengths=[", len(data), marker)
	for i := 0; i < limit; i++ {
		if i > 0 {
			summary += ","
		}
		summary += fmt.Sprintf("%d", len(data[i]))
	}
	if limit < len(data) {
		summary += ",..."
	}
	summary += "]"
	return summary
}

// UnmarshalBitmap wraps roaring64 bitmap deserialization with the same panic
// boundary used for BSI payloads. Roaring treats malformed byte slices as an
// internal invariant violation in some paths, but remote projection payloads
// must surface as regular errors instead of taking down the proxy process.
func UnmarshalBitmap(bitmap *roaring64.Bitmap, data []byte) (err error) {
	if bitmap == nil || len(data) == 0 {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("roaring bitmap unmarshal failed: %v", recovered)
		}
	}()
	return bitmap.UnmarshalBinary(data)
}
