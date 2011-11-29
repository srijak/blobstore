package blobstore

import (
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type Unit struct{}
type Integration struct{}

var _ = Suite(&Unit{})
var _ = Suite(&Integration{})
