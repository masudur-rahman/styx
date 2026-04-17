package pagination

// Paginator holds pagination metadata.
type Paginator struct {
	Page       int64
	PerPage    int64
	TotalItems int64
	TotalPages int64
}

// NewPaginator creates a Paginator with computed TotalPages.
func NewPaginator(page, perPage, totalItems int64) Paginator {
	if perPage <= 0 {
		perPage = 20
	}
	if page <= 0 {
		page = 1
	}
	totalPages := totalItems / perPage
	if totalItems%perPage != 0 {
		totalPages++
	}
	return Paginator{
		Page:       page,
		PerPage:    perPage,
		TotalItems: totalItems,
		TotalPages: totalPages,
	}
}

// HasNext returns true if there are more pages after the current one.
func (p Paginator) HasNext() bool {
	return p.Page < p.TotalPages
}

// HasPrev returns true if the current page is beyond the first.
func (p Paginator) HasPrev() bool {
	return p.Page > 1
}
