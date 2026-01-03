package raft

import "errors"

var (
	ErrNotRunning   = errors.New("raft: not running")
	ErrNodeNotFound = errors.New("raft: node not found in configuration")
)
