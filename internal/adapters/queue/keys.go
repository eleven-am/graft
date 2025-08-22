package queue

import (
	"fmt"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

const (
	keyPrefixQueue      = "queue"
	keyPrefixItem       = "item"
	keyPrefixMeta       = "meta"
	keyPrefixPendingIdx = "pending_idx"
	keySeparator        = ":"
	timestampWidth      = 19
)

func generateQueueKey(queueType QueueType, timestamp time.Time, id string) string {
	nanos := timestamp.UnixNano()
	return fmt.Sprintf("%s:%s:%019d:%s", keyPrefixQueue, queueType, nanos, id)
}

func generateItemDataKey(id string) string {
	return fmt.Sprintf("%s:%s:%s", keyPrefixQueue, keyPrefixItem, id)
}

func generateMetadataKey(queueType QueueType) string {
	return fmt.Sprintf("%s:%s:%s", keyPrefixQueue, keyPrefixMeta, queueType)
}

func getQueuePrefix(queueType QueueType) string {
	return fmt.Sprintf("%s:%s:", keyPrefixQueue, queueType)
}

func parseQueueKey(key string) (queueType QueueType, timestamp time.Time, id string, err error) {
	parts := strings.Split(key, keySeparator)
	if len(parts) != 4 {
		return "", time.Time{}, "", domain.NewValidationError("queue_key", fmt.Sprintf("invalid format: %s", key))
	}

	if parts[0] != keyPrefixQueue {
		return "", time.Time{}, "", domain.NewValidationError("queue_key_prefix", fmt.Sprintf("invalid prefix: %s", parts[0]))
	}

	queueType = QueueType(parts[1])

	var nanos int64
	if _, err := fmt.Sscanf(parts[2], "%019d", &nanos); err != nil {
		return "", time.Time{}, "", domain.NewValidationError("queue_key_timestamp", fmt.Sprintf("invalid timestamp: %s", parts[2]))
	}

	timestamp = time.Unix(0, nanos)
	id = parts[3]

	return queueType, timestamp, id, nil
}

func generatePendingIndexKey(itemID string) string {
	return fmt.Sprintf("%s:%s:%s", keyPrefixQueue, keyPrefixPendingIdx, itemID)
}

func extractIDFromItemKey(key string) (string, error) {
	parts := strings.Split(key, keySeparator)
	if len(parts) != 3 || parts[0] != keyPrefixQueue || parts[1] != keyPrefixItem {
		return "", domain.NewValidationError("item_key", fmt.Sprintf("invalid format: %s", key))
	}
	return parts[2], nil
}
