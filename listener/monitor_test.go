package listener

type monitorMock struct{}

func (m *monitorMock) IncPublishedEvents(subject, table string) {}

func (m *monitorMock) IncFilterSkippedEvents(table string) {}
