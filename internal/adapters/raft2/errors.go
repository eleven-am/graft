package raft2

import "errors"

var (
	errNotImplemented            = errors.New("raft2: not implemented")
	errControllerAlreadyStarted  = errors.New("raft2: controller already started")
	errControllerNotStarted      = errors.New("raft2: controller not started")
	errMissingCoordinator        = errors.New("raft2: bootstrap coordinator dependency missing")
	errMissingRuntime            = errors.New("raft2: runtime dependency missing")
	errMissingReadiness          = errors.New("raft2: readiness gate dependency missing")
	errCoordinatorAlreadyStarted = errors.New("raft2: bootstrap coordinator already started")
	errCoordinatorNotStarted     = errors.New("raft2: bootstrap coordinator not started")
	errNotLeader                 = errors.New("raft2: not leader")
)
