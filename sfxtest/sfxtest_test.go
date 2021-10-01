package sfxtest

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestForcedErr(t *testing.T) {
	Convey("Given an error and names", t, func() {
		err := errors.New("my error")
		names := []string{"foo", "bar"}
		Convey("ForcedError returns a ErrCheck", func() {
			errCheck := ForcedError(err, names...)
			Convey("That returns error when one of names is specified", func() {
				So(errCheck(names[0]), ShouldEqual, err)
			})
			Convey("That does not return error when a string not in names is specified", func() {
				So(errCheck(names[0]+"moo"), ShouldBeNil)
			})
		})
	})
}

type errCheckerTest struct {
	ErrChecker
	name string
}

func (e *errCheckerTest) GetName() (string, error) {
	if err := e.CheckForError("GetName"); err != nil {
		return "", err
	}
	return e.name, nil
}

func (e *errCheckerTest) SetName(name string) error {
	if err := e.CheckForError("SetName"); err != nil {
		return err
	}
	e.name = name
	return nil
}

func TestSetErrorCheck(t *testing.T) {
	Convey("Given a struct using ErrChecker and an ErrCheck", t, func() {
		e := &errCheckerTest{name: "hi"}
		err := errors.New("my error")
		errCheck := ForcedError(err, "GetName")
		Convey("SetErrorCheck(ErrCheck) will make struct use ErrCheck", func() {
			e.SetErrorCheck(errCheck)
			_, err := e.GetName()
			So(err, ShouldEqual, err)
		})
	})
}

func TestCheckForError(t *testing.T) {
	Convey("Given a struct using ErrChecker and an ErrCheck", t, func() {
		e := &errCheckerTest{name: "hi"}
		err := errors.New("my error")
		Convey("With ErrCheck set CheckForErrors uses the ErrCheck", func() {
			e.SetErrorCheck(ForcedError(err, "GetName"))
			_, err := e.GetName()
			So(err, ShouldEqual, err)
			So(e.SetName("new name"), ShouldBeNil)
		})
		Convey("Without ErrCheck set CheckForErrors returns nil", func() {
			_, err := e.GetName()
			So(err, ShouldBeNil)
			So(e.SetName("new name"), ShouldBeNil)
		})
	})
}
