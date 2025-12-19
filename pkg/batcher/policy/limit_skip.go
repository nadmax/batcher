package policy

type LimitSkipPolicy struct {
	limit int
}

func NewLimitSkipPolicy(limit int) *LimitSkipPolicy {
	return &LimitSkipPolicy{
		limit: limit,
	}
}

func (p *LimitSkipPolicy) ShouldSkip(err error, skipCount int) bool {
	if err == nil {
		return false
	}

	return skipCount < p.limit
}
