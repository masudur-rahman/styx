package lib

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/masudur-rahman/database/pkg"
	"github.com/masudur-rahman/database/sql/postgres/pg-grpc/pb"

	"github.com/iancoleman/strcase"
	_ "github.com/lib/pq"
)

func GetPostgresConnection(connStr string) (*sql.Conn, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	if err = conn.PingContext(context.Background()); err != nil {
		return nil, err
	}
	return conn, nil
}

func isDefaultValue(value interface{}) bool {
	typ := reflect.TypeOf(value)
	zero := reflect.Zero(typ).Interface()
	return reflect.DeepEqual(value, zero)
}

func toDBFieldName(fieldName string) string {
	return strcase.ToSnake(fieldName)
}

func fromDBFieldName(fieldName string) string {
	return strcase.ToLowerCamel(fieldName)
}

func toColumnValue(key string, val interface{}) (string, string) {
	key = toDBFieldName(key)
	var value string
	switch v := val.(type) {
	case string:
		value = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case time.Time:
		value = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
	case []string:
		value = fmt.Sprintf("('%s')", strings.Join(v, "', '"))
	case []any:
		value = HandleSliceAny(v)
	default:
		value = fmt.Sprintf("%v", v)
	}

	return key, value
}

func HandleSliceAny(v []any) string {
	var value string
	var vals []string
	typ := reflect.String.String()
	for _, elem := range v {
		if str, ok := elem.(string); ok {
			vals = append(vals, str)
		} else {
			typ = reflect.Interface.String()
			vals = append(vals, fmt.Sprintf("%v", elem))
		}
	}

	if typ == reflect.String.String() {
		value = fmt.Sprintf("('%s')", strings.Join(vals, "', '"))
	} else {
		value = fmt.Sprintf("(%s)", strings.Join(vals, ", "))
	}
	return value
}

func GenerateWhereClauseFromID(id any) string {
	if id == "" {
		return ""
	}

	col, value := toColumnValue("id", id)
	return strings.Join([]string{col, value}, "=")
}

func GenerateWhereClauseFromFilter(filter any) string {
	var conditions []string

	val := reflect.ValueOf(filter)
	for idx := 0; idx < val.NumField(); idx++ {
		field := val.Type().Field(idx)
		if val.Field(idx).IsZero() {
			continue
		}

		fieldName := field.Tag.Get("json")
		if fieldName == "" {
			fieldName = field.Name
		}

		col, value := toColumnValue(fieldName, val.Field(idx).Interface())
		condition := strings.Join([]string{col, value}, "=")
		conditions = append(conditions, condition)
	}
	return strings.Join(conditions, " AND ")
}

func GenerateQuery(tableName, where string) {

}

func GenerateReadQuery(tableName string, filter map[string]interface{}) string {
	var conditions []string

	for key, val := range filter {
		// TODO: Add support for passing field_names to be included in query
		if isDefaultValue(val) {
			// don't insert the default value checks into the condition array
			continue
		}

		col, value := toColumnValue(key, val)

		operator := " = "
		if value[0] == '(' {
			operator = " IN "
		}
		condition := strings.Join([]string{col, value}, operator)
		conditions = append(conditions, condition)
	}

	var conditionString string
	query := fmt.Sprintf("SELECT * FROM \"%s\"", tableName)

	if len(conditions) > 0 {
		conditionString = " WHERE "
		conditionString += strings.Join(conditions, " AND ")
	}

	query += conditionString

	return query
}

func scanSingleRecordAny(rows *sql.Rows, doc any) error {
	fields, err := rows.Columns()
	if err != nil {
		return err
	}
	scans := make([]interface{}, len(fields))

	for i := range scans {
		scans[i] = &scans[i]
	}
	if err = rows.Scan(scans...); err != nil {
		return err
	}
	elem := reflect.ValueOf().Elem()
	elem.

	record := make(map[string]interface{})
	for i := range scans {
		fieldName := fromDBFieldName(fields[i])
		record[fieldName] = scans[i]
	}
}

func scanSingleRecord(rows *sql.Rows) (map[string]interface{}, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	scans := make([]interface{}, len(fields))

	for i := range scans {
		scans[i] = &scans[i]
	}
	if err = rows.Scan(scans...); err != nil {
		return nil, err
	}

	record := make(map[string]interface{})
	for i := range scans {
		fieldName := fromDBFieldName(fields[i])
		record[fieldName] = scans[i]
	}

	return record, nil
}

func ExecuteReadQuery(ctx context.Context, query string, conn *sql.Conn, lim int64) ([]map[string]interface{}, error) {
	log.Printf("Read Query: query=%v\n", query)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]map[string]interface{}, 0)

	for rows.Next() {
		record, err := scanSingleRecord(rows)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
		if lim > 0 && int64(len(records)) >= lim {
			break
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if lim == 1 && len(records) < 1 {
		return nil, sql.ErrNoRows
	}

	return records, nil
}

func GenerateInsertQuery(tableName string, record map[string]interface{}) string {
	var cols []string
	var values []string

	for key, val := range record {
		//if isDefaultValue(val) {
		//	// don't need to insert the default values into the table
		//	continue
		//}

		col, value := toColumnValue(key, val)
		cols = append(cols, col)
		values = append(values, value)
	}

	colClause := strings.Join(cols, ", ")
	valClause := strings.Join(values, ", ")
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, colClause, valClause)

	return query
}

func ExecuteWriteQuery(ctx context.Context, query string, conn *sql.Conn) (sql.Result, error) {
	log.Printf("Write Query: query=%v\n", query)
	result, err := conn.ExecContext(ctx, query)

	return result, err
}

func GenerateUpdateQuery(table string, id string, record map[string]interface{}) string {
	var setValues []string

	for key, val := range record {
		if isDefaultValue(val) {
			// don't add the default values into the set query
			continue
		}
		col, value := toColumnValue(key, val)
		setValue := fmt.Sprintf("%s = %s", col, value)
		setValues = append(setValues, setValue)
	}

	setClause := strings.Join(setValues, ", ")

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = '%s'", table, setClause, id)
	return query
}

func GenerateDeleteQuery(table, id string) string {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", table, id)
	return query
}

func MapToRecord(record map[string]interface{}) (*pb.RecordResponse, error) {
	pm, err := pkg.ToProtoAny(record)
	if err != nil {
		return nil, err
	}

	return &pb.RecordResponse{Record: pm}, nil
}

func MapsToRecords(records []map[string]interface{}) (*pb.RecordsResponse, error) {
	rr := &pb.RecordsResponse{
		Records: make([]*pb.RecordResponse, 0, len(records)),
	}

	for _, record := range records {
		r, err := MapToRecord(record)
		if err != nil {
			return nil, err
		}

		rr.Records = append(rr.Records, r)
	}
	return rr, nil
}
