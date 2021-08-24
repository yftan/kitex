/*
 * Copyright 2021 tanyufeng Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thrift

import (
	"context"
	"fmt"
	"github.com/bitly/go-simplejson"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
)

// NewWriteJSON ...
func NewWriteJSON(svc *descriptor.ServiceDescriptor, method string, isClient bool) (*WriteJSON, error) {
	fnDsc, err := svc.LookupFunctionByMethod(method)
	if err != nil {
		return nil, err
	}
	ty := fnDsc.Request
	if !isClient {
		ty = fnDsc.Response
	}
	ws := &WriteJSON{
		ty:             ty,
		hasRequestBase: fnDsc.HasRequestBase && isClient,
	}
	return ws, nil
}

// WriteJSON implement of MessageWriter
type WriteJSON struct {
	ty             *descriptor.TypeDescriptor
	hasRequestBase bool
}

var _ MessageWriter = (*WriteJSON)(nil)

// Write ...
func (m *WriteJSON) Write(ctx context.Context, out thrift.TProtocol, msg interface{}, requestBase *Base) error {
	if !m.hasRequestBase {
		requestBase = nil
	}
	body, err := simplejson.NewJson([]byte(msg.(string)))
	if err != nil {
		return err
	}
	return wrapStructWriter(ctx, body, out, m.ty, &writerOption{requestBase: requestBase})
}

// NewReadJSON ...
func NewReadJSON(svc *descriptor.ServiceDescriptor, isClient bool) *ReadJSON {
	return &ReadJSON{
		svc:      svc,
		isClient: isClient,
	}
}

// ReadJSON implement of MessageReaderWithMethod
type ReadJSON struct {
	svc      *descriptor.ServiceDescriptor
	isClient bool
}

var _ MessageReader = (*ReadJSON)(nil)

// Read ...
func (m *ReadJSON) Read(ctx context.Context, method string, in thrift.TProtocol) (interface{}, error) {
	fnDsc, err := m.svc.LookupFunctionByMethod(method)
	if err != nil {
		return nil, err
	}
	fDsc := fnDsc.Response
	if !m.isClient {
		fDsc = fnDsc.Request
	}
	resp, err := skipStructReader(ctx, in, fDsc, &readerOption{forJSON: true, throwException: true})
	if err != nil {
		return nil, err
	}
	respJSON, ok := resp.(*simplejson.Json)
	if !ok {
		return nil, fmt.Errorf("response is not simple json. response:%#v", resp)
	}
	encode, err := respJSON.Encode()
	if err != nil {
		return nil, err
	}
	return string(encode), nil
}
