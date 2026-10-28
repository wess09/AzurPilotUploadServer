package main

// Cloud responses are passed through. No application pacing, reservation,
// concurrency gate or proactive endpoint cooldown remains on direct uploads.
func (s *Server) cloudStatus() map[string]any {
	return map[string]any{"application_throttling": false, "concurrency_limit": 0, "qps_limit": 0, "proactive_backoff": false}
}
