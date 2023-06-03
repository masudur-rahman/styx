package dberr

func CheckEntityNameNonEmpty(entity string) error {
	if entity == "" {
		return NewRequirementMissing("entity name must be set")
	}
	return nil
}

func CheckIDNonEmpty(id string) error {
	if id == "" {
		return NewRequirementMissing("must provide document id")
	}
	return nil
}

func CheckIdOrFilterNonEmpty(id string, filter interface{}) error {
	if id == "" && filter == nil {
		return NewRequirementMissing("must provide id and/or filter")
	}
	return nil
}
