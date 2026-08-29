package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/masudur-rahman/styx/v2/pkg"
	core "github.com/masudur-rahman/styx/v2/sql/internal/core"
	"github.com/masudur-rahman/styx/v2/sql/postgres/pg-grpc/pb"

	"github.com/iancoleman/strcase"
)

func toDBFieldName(fieldName string) string {
	return strcase.ToSnake(fieldName)
}

func fromDBFieldName(fieldName string) string {
	return strcase.ToLowerCamel(fieldName)
}

func MapToRecord(record map[string]any) (*pb.RecordResponse, error) {
	pm, err := pkg.ToProtoAny(record)
	if err != nil {
		return nil, err
	}

	return &pb.RecordResponse{Record: pm}, nil
}

func MapsToRecords(records []map[string]any) (*pb.RecordsResponse, error) {
	var rs []*pb.RecordResponse
	for _, r := range records {
		pm, err := pkg.ToProtoAny(r)
		if err != nil {
			return nil, err
		}
		rs = append(rs, &pb.RecordResponse{Record: pm})
	}
	return &pb.RecordsResponse{Records: rs}, nil
}

func GenerateReadQuery(tableName string, record map[string]any) string {
	var conditions []string
	for key, val := range record {
		if core.IsZeroValue(val) {
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

func ExecuteReadQuery(ctx context.Context, query string, conn *sql.DB, lim int64) ([]map[string]any, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]map[string]any, 0)
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

func scanSingleRecord(rows *sql.Rows) (map[string]any, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	scans := make([]any, len(fields))
	for i := range scans {
		scans[i] = &scans[i]
	}
	if err = rows.Scan(scans...); err != nil {
		return nil, err
	}

	record := make(map[string]any)
	for i := range scans {
		fieldName := fromDBFieldName(fields[i])
		record[fieldName] = scans[i]
	}
	return record, nil
}

func GenerateInsertQuery(tableName string, record map[string]any) string {
	var cols []string
	var values []string
	for key, val := range record {
		col, value := toColumnValue(key, val)
		cols = append(cols, col)
		values = append(values, value)
	}
	return fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, strings.Join(cols, ", "), strings.Join(values, ", "))
}

func GenerateUpdateQuery(table string, id string, record map[string]any) string {
	var setValues []string
	for key, val := range record {
		if core.IsZeroValue(val) {
			continue
		}
		col, value := toColumnValue(key, val)
		setValues = append(setValues, fmt.Sprintf("%s = %s", col, value))
	}
	return fmt.Sprintf("UPDATE \"%s\" SET %s WHERE id = '%s'", table, strings.Join(setValues, ", "), id)
}

func GenerateDeleteQuery(table, id string) string {
	return fmt.Sprintf("DELETE FROM \"%s\" WHERE id = '%s'", table, id)
}

func toColumnValue(key string, val any) (string, string) {
	key = strcase.ToSnake(key)
	value := formatValues(val)
	return key, value
}

func formatValues(val any) string {
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
	case []string:
		return fmt.Sprintf("('%s')", strings.Join(v, "', '"))
	default:
		return fmt.Sprintf("%v", v)
	}
}
