package log

type ErrorLogger interface {
	Log(keyvals ...interface{}) error
}

var DefaultErrorHandler ErrorHandler = Discard

type ErrorLogLogger struct {
	RootLogger ErrorLogger
	ErrHandler ErrorHandler
}

func (e *ErrorLogLogger) Log(keyvals ...interface{}) {
	if err := e.RootLogger.Log(keyvals...); err != nil && e.ErrHandler != nil {
		e.ErrorLogger(err).Log(keyvals...)
	}
}

func (e *ErrorLogLogger) ErrorLogger(err error) Logger {
	return e.ErrHandler.ErrorLogger(err)
}

var _ ErrorHandlingLogger = &ErrorLogLogger{}

func FromGokit(logger ErrorLogger) *ErrorLogLogger {
	return &ErrorLogLogger {
		RootLogger: logger,
		ErrHandler: DefaultErrorHandler,
	}
}

type LoggerKit struct {
	LogTo Logger
}

func ToGokit(logger Logger) ErrorLogger {
	if root, ok := logger.(*ErrorLogLogger); ok {
		return root.RootLogger
	}
	return &LoggerKit {
		LogTo: logger,
	}
}

var _ ErrorLogger = &LoggerKit{}

func (k *LoggerKit) Log(keyvals ...interface{}) error {
	k.LogTo.Log(keyvals...)
	return nil
}