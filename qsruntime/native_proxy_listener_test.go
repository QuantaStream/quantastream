package qsruntime

import (
	"context"
	"errors"
	"testing"
)

func TestNativeProxyListenConfigDefaultsFromFrontDoor(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{BindAddress: "0.0.0.0", Port: 4400})
	config := (NativeProxyListenConfig{}).WithDefaults(frontDoor)
	if config.Address != "0.0.0.0:4400" {
		t.Fatalf("address = %q", config.Address)
	}
}

func TestNativeProxyFrontDoorListenAndServeIsDisabledScaffold(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})
	err := frontDoor.ListenAndServe(context.Background(), NativeProxyListenConfig{})
	if !errors.Is(err, ErrNativeProxyListenerNotReady) {
		t.Fatalf("err = %v", err)
	}
}
