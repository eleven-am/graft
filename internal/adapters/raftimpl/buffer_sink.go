package raftimpl

import "bytes"

type BufferSink struct {
	bytes.Buffer
}

func (b *BufferSink) Close() error {
	return nil
}

func (b *BufferSink) ID() string {
	return "buffer-sink"
}

func (b *BufferSink) Cancel() error {
	return nil
}