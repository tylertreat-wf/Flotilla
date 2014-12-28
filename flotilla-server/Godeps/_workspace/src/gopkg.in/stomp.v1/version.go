package stomp

// Version is the STOMP protocol version.
type Version string

const (
	V10 Version = "1.0"
	V11 Version = "1.1"
	V12 Version = "1.2"
)

// String returns a string representation of the STOMP version.
func (v Version) String() string {
	return string(v)
}

// SupportsNack indicates whether this version of the STOMP protocol
// supports use of the NACK command.
func (v Version) SupportsNack() bool {
	switch v {
	case V10:
		return false
	case V11, V12:
		return true
	}
	panic("invalid version: " + v)
}
