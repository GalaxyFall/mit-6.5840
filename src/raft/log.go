package raft

type Log struct {
	Entries []LogEntry
}

func newLog() *Log {
	l := LogEntry{Term: 0}
	return &Log{
		Entries: []LogEntry{l},
	}
}

func (l *Log) GetStart() int {
	return l.Entries[0].Index
}

func (l *Log) Len() int {
	return len(l.Entries)
}

func (l *Log) Append(e []LogEntry) {
	l.Entries = append(l.Entries, e...)
}

func (l *Log) TrimLog(index int) {
	l.Entries = append([]LogEntry{}, l.Entries[:index]...)
}

func (l *Log) GetFirst() LogEntry {
	return l.Entries[0]
}

func (l *Log) GetLastIndex() int {
	return l.Entries[l.Len()-1].Index
}

func (l *Log) GetLastTerm() int {
	return l.Entries[l.Len()-1].Term
}

func (l *Log) GetEntry(index int) LogEntry {
	return l.Entries[index]
}

func (l *Log) GetLastLog() LogEntry {
	return l.Entries[l.Len()-1]
}

func (l *Log) Copy(index int) []LogEntry {
	entries := make([]LogEntry, len(l.Entries[index:]))
	copy(entries, l.Entries[index:])
	return entries
}
