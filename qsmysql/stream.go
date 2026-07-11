package qsmysql

import (
	"context"
	"fmt"
	"io"
)

// Stream adapts an io.Reader/io.Writer pair to MySQL packet framing.
type Stream struct {
	Reader io.Reader
	Writer io.Writer
}

// NewStream returns a packet stream over the supplied reader and writer.
func NewStream(reader io.Reader, writer io.Writer) Stream {
	return Stream{Reader: reader, Writer: writer}
}

// ReadPacket reads exactly one MySQL packet from the stream.
func (s Stream) ReadPacket(ctx context.Context) (Packet, error) {
	if err := ctx.Err(); err != nil {
		return Packet{}, err
	}
	if s.Reader == nil {
		return Packet{}, fmt.Errorf("mysql packet stream reader is nil")
	}
	header := make([]byte, PacketHeaderLength)
	if _, err := io.ReadFull(s.Reader, header); err != nil {
		return Packet{}, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(s.Reader, payload); err != nil {
		return Packet{}, err
	}
	return Packet{SequenceID: header[3], Payload: payload}, nil
}

// WritePacket writes exactly one MySQL packet to the stream.
func (s Stream) WritePacket(ctx context.Context, packet Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Writer == nil {
		return fmt.Errorf("mysql packet stream writer is nil")
	}
	encoded, err := EncodePacket(packet)
	if err != nil {
		return err
	}
	written, err := s.Writer.Write(encoded)
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}
