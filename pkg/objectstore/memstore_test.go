/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package objectstore

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// memStore is an in-memory Store implementation used across objectstore tests.
// It models the prefix-joining contract so callers can assert end-to-end key
// behavior without any cloud SDK.
type memStore struct {
	scheme string
	prefix string

	mu      sync.Mutex
	objects map[string][]byte
	times   map[string]time.Time
	clock   time.Time
}

func newMemStore(scheme string) *memStore {
	return &memStore{
		scheme:  scheme,
		objects: map[string][]byte{},
		times:   map[string]time.Time{},
		clock:   time.Unix(0, 0),
	}
}

func (m *memStore) Scheme() string { return m.scheme }

func (m *memStore) Upload(_ context.Context, key string, r io.Reader, _ int64) (UploadResult, error) {
	full := JoinKey(m.prefix, key)
	data, err := io.ReadAll(r)
	if err != nil {
		return UploadResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[full] = data
	m.clock = m.clock.Add(time.Second)
	m.times[full] = m.clock
	return UploadResult{
		URI:  fmt.Sprintf("%s://%s", m.scheme, full),
		Size: int64(len(data)),
	}, nil
}

func (m *memStore) List(_ context.Context, keyPrefix string) ([]ObjectInfo, error) {
	full := JoinKey(m.prefix, keyPrefix)
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ObjectInfo
	for k, v := range m.objects {
		if len(full) == 0 || hasPrefix(k, full) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(v)), LastModified: m.times[k]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastModified.After(out[j].LastModified) })
	return out, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	full := JoinKey(m.prefix, key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[full]; !ok {
		return fmt.Errorf("memstore: object %q not found", full)
	}
	delete(m.objects, full)
	delete(m.times, full)
	return nil
}

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
