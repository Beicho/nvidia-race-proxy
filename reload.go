package main

import "fmt"

// Slots remain stable while requests hold them, including across key removals.
func (s *scheduler) reload(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySecret := make(map[string]int, len(s.keys))
	for i := range s.keys {
		bySecret[s.keys[i].secret] = i
		s.keys[i].retired = true
	}
	for _, secret := range keys {
		if index, exists := bySecret[secret]; exists {
			s.keys[index].retired = false
		} else {
			bySecret[secret] = len(s.keys)
			s.keys = append(s.keys, keyState{secret: secret})
		}
	}
}

func (s *proxyServer) reloadKeys() error {
	keys, err := loadKeys(s.cfg.KeysFile)
	if err == nil && len(keys) < s.cfg.Fanout {
		err = fmt.Errorf("key pool is smaller than configured fanout")
	}
	if err != nil {
		s.metrics.reload(false)
		return err
	}
	s.scheduler.reload(keys)
	s.metrics.reload(true)
	return nil
}
