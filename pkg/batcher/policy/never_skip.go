package policy

type NeverSkipPolicy struct{}

func NewNeverSkipPolicy() *NeverSkipPolicy {
	return &NeverSkipPolicy{}
}

func (p *NeverSkipPolicy) ShouldSkip(err error, skipCount int) bool {
	return false
}
