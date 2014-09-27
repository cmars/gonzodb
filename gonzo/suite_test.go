package gonzo_test

import (
	"net"
	"testing"

	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

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

var queryMatchTestCases = []bson.D{
	bson.D{{"artist", "ed hall"}, {"label", "trance syndicate"}, {"venue", "liberty lunch"}},
	bson.D{{"artist", "cherubs"}, {"label", "trance syndicate"}, {"venue", "cavity club"}},
	bson.D{{"artist", "the jesus lizard"}, {"label", "touch & go"}, {"venue", "emo's"}},
}

func (s *gonzoSuite) TestInsertQueryMatch(c *gc.C) {
	for _, testCase := range queryMatchTestCases {
		err := s.session.DB("db1").C("c1").Insert(testCase)
		c.Assert(err, gc.IsNil)
	}

	var err error
	var result []bson.M
	err = s.session.DB("db1").C("c1").Find(bson.M{"artist": "ed hall"}).All(&result)
	c.Assert(err, gc.IsNil)
	c.Assert(result, gc.HasLen, 1)

	err = s.session.DB("db1").C("c1").Find(bson.M{"label": "trance syndicate"}).All(&result)
	c.Assert(err, gc.IsNil)
	c.Assert(result, gc.HasLen, 2)
	for i, m := range result {
		c.Assert(m["label"], gc.Equals, "trance syndicate")
		if i > 0 {
			c.Assert(m["artist"], gc.Not(gc.DeepEquals), result[i-1]["artist"])
			c.Assert(m["venue"], gc.Not(gc.DeepEquals), result[i-1]["venue"])
			c.Assert(m["_id"], gc.Not(gc.DeepEquals), result[i-1]["_id"])
		}
	}

	err = s.session.DB("db1").C("c1").Find(nil).All(&result)
	c.Assert(err, gc.IsNil)
	c.Assert(result, gc.HasLen, 3)
}

func (s *gonzoSuite) TestQueryIter(c *gc.C) {
	count := 1000
	for i := 0; i < count; i++ {
		err := s.session.DB("db1").C("c1").Insert(bson.M{"i": i})
		c.Assert(err, gc.IsNil)
	}

	i := s.session.DB("db1").C("c1").Find(nil).Iter()
	var m bson.M
	n := 0
	for i.Next(&m) {
		c.Assert(m, gc.HasLen, 2)
		_, ok := m["_id"]
		c.Assert(ok, gc.Equals, true)
		n++
	}
	c.Assert(n, gc.Equals, count)
	c.Assert(i.Err(), gc.IsNil)
	c.Assert(i.Close(), gc.IsNil)
}

func (s *gonzoSuite) TestGridFSrt(c *gc.C) {
	gfs := s.session.DB("whatfs").GridFS("whatroot")

	fooFile, err := gfs.Create("foo")
	c.Assert(err, gc.IsNil)
	_, err = fooFile.Write([]byte("this file contains foo"))
	c.Assert(err, gc.IsNil)
	c.Assert(fooFile.Close(), gc.IsNil)

	fooFile, err = gfs.Open("foo")
	c.Assert(err, gc.IsNil)
	buf := make([]byte, 200)
	n, err := fooFile.Read(buf)
	c.Assert(err, gc.IsNil)
	c.Assert(string(buf[:n]), gc.Equals, "this file contains foo")

	_, err = gfs.Open("bar")
	c.Assert(err, gc.ErrorMatches, "not found")
}

func (s *gonzoSuite) TestCountUpdateReplace(c *gc.C) {
	for _, testCase := range queryMatchTestCases {
		err := s.session.DB("db1").C("c1").Insert(testCase)
		c.Assert(err, gc.IsNil)
	}

	n, err := s.session.DB("db1").C("c1").Find(bson.M{"artist": "ed hall"}).Count()
	c.Assert(err, gc.IsNil)
	c.Assert(n, gc.Equals, 1)

	err = s.session.DB("db1").C("c1").Update(bson.M{"artist": "ed hall"},
		bson.D{{"artist", "fugazi"}, {"label", "dischord"}, {"venue", "liberty lunch"}})
	c.Assert(err, gc.IsNil)

	var result []bson.M
	err = s.session.DB("db1").C("c1").Find(bson.D{{"artist", "ed hall"}}).All(&result)
	c.Assert(err, gc.ErrorMatches, "not found")
	err = s.session.DB("db1").C("c1").Find(bson.D{{"venue", "liberty lunch"}}).All(&result)
	c.Assert(err, gc.IsNil)
	c.Assert(result, gc.HasLen, 1)
	c.Assert(result[0]["artist"], gc.Equals, "fugazi")
}
