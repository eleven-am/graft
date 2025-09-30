package raft

import "errors"

var (
	errNotImplemented            = errors.New("raft: not implemented")
	errControllerAlreadyStarted  = errors.New("raft: controller already started")
	errControllerNotStarted      = errors.New("raft: controller not started")
	errMissingCoordinator        = errors.New("raft: bootstrap coordinator dependency missing")
	errMissingRuntime            = errors.New("raft: runtime dependency missing")
	errMissingReadiness          = errors.New("raft: readiness gate dependency missing")
	errCoordinatorAlreadyStarted = errors.New("raft: bootstrap coordinator already started")
	errCoordinatorNotStarted     = errors.New("raft: bootstrap coordinator not started")
	errNotLeader                 = errors.New("raft: not leader")
)
