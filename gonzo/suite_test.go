package gonzo_test

import (
	"net"
	"testing"

	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"gopkg.in/mgo.v2/bson"

	"github.com/cmars/gonzodb/gonzo"
)

func Test(t *testing.T) {
	gc.TestingT(t)
}

type gonzoSuite struct {
	server  *gonzo.Server
	session *mgo.Session
}

var _ = gc.Suite(&gonzoSuite{})

func (s *gonzoSuite) SetUpTest(c *gc.C) {
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
	l, err := net.ListenTCP("tcp", addr)
	c.Assert(err, gc.IsNil)
	addr = l.Addr().(*net.TCPAddr)

	s.server = gonzo.NewServer(l)
	s.server.Start()

	s.session, err = mgo.Dial(addr.String())
	c.Assert(err, gc.IsNil)
}

func (s *gonzoSuite) TearDownTest(c *gc.C) {
	s.session.Close()
	s.server.Stop()
}

func (s *gonzoSuite) TestInsertQuerySingle(c *gc.C) {
	err := s.session.DB("db1").C("c1").Insert(bson.D{{"foo", 1}, {"bar", 2}})
	c.Assert(err, gc.IsNil)

	var result []bson.M
	err = s.session.DB("db1").C("c1").Find(nil).All(&result)
	c.Assert(err, gc.IsNil)
	c.Assert(result, gc.HasLen, 1)
	c.Assert(result[0]["foo"], gc.DeepEquals, 1)
	c.Assert(result[0]["bar"], gc.DeepEquals, 2)
	_, ok := result[0]["_id"].(bson.ObjectId)
	c.Assert(ok, gc.Equals, true)
}
