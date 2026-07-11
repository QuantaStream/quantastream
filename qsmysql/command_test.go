package qsmysql

import "testing"

func TestDecodeCommandQuery(t *testing.T) {
	command, err := DecodeCommand(append([]byte{byte(CommandQuery)}, []byte(" select 1 ")...))
	if err != nil {
		t.Fatalf("DecodeCommand failed: %v", err)
	}
	if command.Kind != CommandKindQuery || command.SQL != " select 1 " {
		t.Fatalf("command = %#v", command)
	}
}

func TestDecodeCommandPingAndQuit(t *testing.T) {
	for _, tc := range []struct {
		payload []byte
		want    CommandKind
	}{
		{[]byte{byte(CommandPing)}, CommandKindPing},
		{[]byte{byte(CommandQuit)}, CommandKindQuit},
	} {
		command, err := DecodeCommand(tc.payload)
		if err != nil {
			t.Fatalf("DecodeCommand(%v) failed: %v", tc.payload, err)
		}
		if command.Kind != tc.want {
			t.Fatalf("command = %#v, want %s", command, tc.want)
		}
	}
}

func TestDecodeCommandRejectsUnsupportedCommand(t *testing.T) {
	if _, err := DecodeCommand([]byte{0xff}); err == nil {
		t.Fatal("expected unsupported command to fail")
	}
}

func TestDecodeCommandQueryRejectsWhitespaceOnlySQL(t *testing.T) {
	if _, err := DecodeCommand(append([]byte{byte(CommandQuery)}, []byte(" \t ")...)); err == nil {
		t.Fatal("expected whitespace-only COM_QUERY to fail")
	}
}
