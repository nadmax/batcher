package policy

type SkipPolicy interface {
	ShouldSkip(err error, skipCount int) bool
}
