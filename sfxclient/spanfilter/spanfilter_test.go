package spanfilter

import (
	"context"
	"github.com/signalfx/golib/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test(t *testing.T) {
	Convey("test spanfilter", t, func() {
		body := []byte(`{"Valid":0}`)
		sf := FromBytes(body)
		So(sf, ShouldNotBeNil)
		So(sf.CheckInvalid(), ShouldBeFalse)
		sf.Add("ok", "")
		So(sf.Valid, ShouldEqual, 1)
		So(ReturnInvalidOrError([]byte(sf.Error())), ShouldBeNil)
		sf.Add("notok", "")
		So(sf.CheckInvalid(), ShouldBeTrue)
		So(FromBytes([]byte("fdsf")), ShouldBeNil)
		So(ReturnInvalidOrError([]byte(sf.Error())).Error(), ShouldEqual, sf.Error())
		So(ReturnInvalidOrError([]byte("blarg")).Error(), ShouldEqual, "blarg")
		So(IsInvalid(sf), ShouldBeTrue)
		So(IsInvalid(errors.New("nope")), ShouldBeTrue)
		So(IsMap(sf), ShouldBeTrue)
		So(IsMap(errors.New("nope")), ShouldBeFalse)
		ctx := WithSpanFilterContext(context.Background(), sf)
		So(ctx, ShouldNotBeNil)
		err := GetSpanFilterMapFromContext(ctx)
		So(err, ShouldNotBeNil)
		So(IsInvalid(err), ShouldBeTrue)
		So(IsInvalid(GetSpanFilterMapFromContext(context.Background())), ShouldBeFalse)
		ctx, err = GetSpanFilterMapOrNew(context.Background())
		So(err.Error(), ShouldEqual, `{"valid":0}`)
		_, err1 := GetSpanFilterMapOrNew(ctx)
		So(err, ShouldEqual, err1)
	})
}
