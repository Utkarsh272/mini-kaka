package protocol

// APIKey identifies the request type. One byte on the wire.
type APIKey uint8

const (
	APIKeyProduce       APIKey = 0
	APIKeyFetch         APIKey = 1
	APIKeyMetadata      APIKey = 2
	APIKeyJoinGroup     APIKey = 3
	APIKeySyncGroup     APIKey = 4
	APIKeyHeartbeat     APIKey = 5
	APIKeyOffsetCommit  APIKey = 6
	APIKeyOffsetFetch   APIKey = 7
	APIKeyFetchFollower APIKey = 8
	APIKeyLeaveGroup    APIKey = 9
	APIKeyCreateTopic   APIKey = 10
	APIKeyDescribeGroup APIKey = 11
)

// ErrorCode is a two-byte error code in every response.
type ErrorCode int16

const (
	ErrNone                  ErrorCode = 0
	ErrUnknown               ErrorCode = -1
	ErrOffsetOutOfRange      ErrorCode = 1
	ErrUnknownTopicPartition ErrorCode = 3
	ErrLeaderNotAvailable    ErrorCode = 5
	ErrNotLeaderForPartition ErrorCode = 6
	ErrRequestTimedOut       ErrorCode = 7
	ErrTopicAlreadyExists    ErrorCode = 36
	ErrInvalidRequest        ErrorCode = 42
)

func (e ErrorCode) Error() string {
	switch e {
	case ErrNone:
		return "none"
	case ErrOffsetOutOfRange:
		return "offset out of range"
	case ErrUnknownTopicPartition:
		return "unknown topic or partition"
	case ErrLeaderNotAvailable:
		return "leader not available"
	case ErrNotLeaderForPartition:
		return "not leader for partition"
	case ErrRequestTimedOut:
		return "request timed out"
	case ErrTopicAlreadyExists:
		return "topic already exists"
	case ErrInvalidRequest:
		return "invalid request"
	default:
		return "unknown error"
	}
}
