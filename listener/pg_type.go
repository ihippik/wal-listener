package listener

// PostgreSQL OIDs
// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
const (
	Int2OID = 21
	Int4OID = 23
	Int8OID = 20

	TextOID    = 25
	VarcharOID = 1043

	TimestampOID   = 1114
	TimestamptzOID = 1184
	DateOID        = 1082
	TimeOID        = 1083

	JSONBOID = 3802
	UUIDOID  = 2950
	BoolOID  = 16

	TSVectorOID = 3614
)
