package handler

// WithConcurrentLimiter will limit the number of concurrent goroutines
func WithConcurrentLimiter(limit uint64) Handler {
	ch := make(chan struct{}, limit)
	return func(c *Context) error {
		ch <- struct{}{}
		defer func() {
			<-ch
		}()

		return c.Next()
	}
}
