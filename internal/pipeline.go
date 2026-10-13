/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
   Author : <blobfusedev@microsoft.com>

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE
*/

package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
)

// Pipeline: Base pipeline structure holding list of components deployed along with the head of pipeline
type Pipeline struct {
	components []Component
	Header     Component
}

// NewComponent : Function that all components have to register to allow their instantiation
type NewComponent func() Component

// Map holding all possible components along with their respective constructors
var registeredComponents map[string]NewComponent

func GetComponent(name string) Component {
	compInit, ok := registeredComponents[name]
	if ok {
		return compInit()
	}
	return nil
}

// NewPipeline : Using a list of strings holding name of components, create and configure the component objects
func NewPipeline(components []string, isParent bool) (*Pipeline, error) {
	comps := make([]Component, 0)
	lastPriority := EComponentPriority.Producer()
	for _, name := range components {
		if name == "stream" {
			common.IsStream = true
			name = "block_cache"
		}
		//  Search component exists in our registered map or not
		compInit, ok := registeredComponents[name]
		if ok {
			// Call the constructor method registered by the component
			comp := compInit()

			// request component to parse and validate config of its interest
			err := comp.Configure(isParent)
			if err != nil {
				log.Err("Pipeline: error creating pipeline component %s [%s]", comp.Name(), err)
				return nil, err
			}

			if comp.Priority() > lastPriority {
				log.Err("Pipeline::NewPipeline : Invalid Component order [priority of %s higher than above components]", comp.Name())
				return nil, fmt.Errorf("config error in Pipeline [component %s is out of order]", name)
			} else {
				lastPriority = comp.Priority()
			}

			// store the configured object in list of components
			comps = append(comps, comp)
		} else {
			log.Err("Pipeline: error [component %s not registered]", name)
			return nil, fmt.Errorf("config error in Pipeline [component %s not registered]", name)
		}

	}

	// Create pipeline structure holding list of all component objects requested by config file
	return &Pipeline{
		components: comps,
	}, nil
}

// NewPipelineWithComponents builds a pipeline from ready-made components.
// Intended for shutdown orchestration unit tests that inject fakes.
func NewPipelineWithComponents(components []Component) *Pipeline {
	p := &Pipeline{components: components}
	p.Header = components[0]
	curComp := components[0]
	for i := 1; i < len(components); i++ {
		curComp.SetNextComponent(components[i])
		curComp = components[i]
	}
	return p
}

// Create : Use Initialize objects to form a pipeline by registering next component to each component
func (p *Pipeline) Create() {
	p.Header = p.components[0]
	curComp := p.Header

	for i := 1; i < len(p.components); i++ {
		nextComp := p.components[i]
		curComp.SetNextComponent(nextComp)
		curComp = nextComp
	}
}

// Start : Start the pipeline by calling 'Start' method of each component in reverse order of chaining
func (p *Pipeline) Start(ctx context.Context) (err error) {
	p.Create()

	var errs []error

	for i := len(p.components) - 1; i >= 0; i-- {
		if err = p.components[i].Start(ctx); err != nil {
			errs = append(errs, err)
			// stop all the upstream components before returning, f.e., this would prevent the upstream components
			// to use the logger after it is destroyed.
			for j := i + 1; j < len(p.components); j++ {
				if err = p.components[j].Stop(); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// Stop : Stop the pipeline by calling 'Stop' method of each component
func (p *Pipeline) Stop() (err error) {
	var errs []error
	for i := range p.components {
		if err = p.components[i].Stop(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// Components returns the ordered component list (libfuse first, producer
// last). The slice is a copy; callers must not mutate the pipeline itself.
func (p *Pipeline) Components() []Component {
	out := make([]Component, len(p.components))
	copy(out, p.components)
	return out
}

// Drainers returns components implementing ShutdownDrainer, in pipeline order.
func (p *Pipeline) Drainers() []ShutdownDrainer {
	out := make([]ShutdownDrainer, 0, len(p.components))
	for _, c := range p.components {
		if d, ok := c.(ShutdownDrainer); ok {
			out = append(out, d)
		}
	}
	return out
}

// DeferredCleaners returns components implementing DeferredCacheCleaner.
func (p *Pipeline) DeferredCleaners() []DeferredCacheCleaner {
	out := make([]DeferredCacheCleaner, 0, len(p.components))
	for _, c := range p.components {
		if d, ok := c.(DeferredCacheCleaner); ok {
			out = append(out, d)
		}
	}
	return out
}

// ByName finds a component by Name(), or nil if absent.
func (p *Pipeline) ByName(name string) Component {
	for _, c := range p.components {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

// StopWithDeadline stops components in pipeline order but bounds each Stop()
// call so a stuck component cannot hang shutdown. Timed-out calls leave an
// abandoned goroutine behind (the process exits right after), but the next
// component still runs. All errors, including timeouts, are joined.
func (p *Pipeline) StopWithDeadline(perComponent time.Duration) error {
	var errs []error
	for _, comp := range p.components {
		done := make(chan error, 1)
		go func(c Component) {
			defer func() {
				if r := recover(); r != nil {
					done <- fmt.Errorf("component %s Stop panic: %v", c.Name(), r)
				}
			}()
			done <- c.Stop()
		}(comp)

		var err error
		if perComponent <= 0 {
			err = <-done
		} else {
			timer := time.NewTimer(perComponent)
			select {
			case err = <-done:
				timer.Stop()
			case <-timer.C:
				err = fmt.Errorf("component %s Stop timed out after %v", comp.Name(), perComponent)
			}
		}
		if err != nil {
			log.Err("Pipeline::StopWithDeadline : component %s Stop error: %s", comp.Name(), err.Error())
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// AddComponent : Each component calls this method in their init to register the constructor
func AddComponent(name string, init NewComponent) {
	registeredComponents[name] = init
}

func init() {
	registeredComponents = make(map[string]NewComponent)
}
