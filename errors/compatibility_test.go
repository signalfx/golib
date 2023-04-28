package errors

import (
	"errors"
	"testing"

	dropboxerrors "github.com/dropbox/godropbox/errors"
	facebookerrors "github.com/facebookgo/stackerr"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGoDropbox(t *testing.T) {
	Convey("When the original error is godropbox", t, func() {
		root := dropboxerrors.New("dropbox root error")
		So(Tail(root), ShouldEqual, root)
		dropboxWrap := dropboxerrors.Wrap(root, "Wrapped error")
		So(Tail(dropboxWrap), ShouldEqual, root)
		var myAnnotation *ChainError
		So(errors.As(Annotate(dropboxWrap, "I have annotated dropbox error"), &myAnnotation), ShouldBeTrue)
		So(Tail(myAnnotation), ShouldEqual, root)
		So(Cause(myAnnotation), ShouldEqual, root)
		So(Details(myAnnotation), ShouldContainSubstring, "dropbox root error")
		So(Details(myAnnotation), ShouldContainSubstring, "I have annotated dropbox error")

		So(myAnnotation.Cause(), ShouldEqual, myAnnotation.Tail())
		So(myAnnotation.Message(), ShouldEqual, myAnnotation.Head().Error())
		So(myAnnotation.Underlying(), ShouldEqual, myAnnotation.Next())

		So(myAnnotation.GetMessage(), ShouldEqual, "I have annotated dropbox error")
		So(myAnnotation.GetInner().Error(), ShouldEqual, "I have annotated dropbox error")

		So(dropboxerrors.RootError(myAnnotation).Error(), ShouldEqual, Tail(myAnnotation).Error())
	})
}

func TestFacebookErrors(t *testing.T) {
	Convey("When the original error is fb", t, func() {
		root := facebookerrors.New("fb root error")
		So(Tail(root), ShouldEqual, root)
		fbWrap := facebookerrors.Wrap(root)
		So(Tail(fbWrap), ShouldEqual, root)
		myAnnotation := Annotate(fbWrap, "I have annotated fb error")

		So(Tail(myAnnotation), ShouldEqual, root)
		So(Cause(myAnnotation), ShouldEqual, root)
		So(Details(myAnnotation), ShouldContainSubstring, "fb root error")
		So(Details(myAnnotation), ShouldContainSubstring, "I have annotated fb error")

		u := facebookerrors.Underlying(myAnnotation)
		last := u[len(u)-1]
		So(last.Error(), ShouldContainSubstring, "fb root error")
		So(last.Error(), ShouldNotContainSubstring, "I have annotated fb error")
	})
}
