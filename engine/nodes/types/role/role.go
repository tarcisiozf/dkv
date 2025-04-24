package role

import "fmt"

var mapRole = map[string]Role{}

var (
	Idle        = addRole("idle")
	Writer      = addRole("writer")
	ReadReplica = addRole("read-replica")
)

func addRole(role string) Role {
	r := Role{role}
	mapRole[role] = r
	return r
}

type Role struct {
	role string
}

func (r Role) String() string {
	return r.role
}

func (r Role) Equal(other Role) bool {
	return r.role == other.role
}

func (r Role) IsReadReplica() bool {
	return r.role == ReadReplica.role
}

func (r Role) IsWriter() bool {
	return r.role == Writer.role
}

func (r Role) IsNull() bool {
	return r.role == ""
}

func Parse(role string) (Role, error) {
	if r, ok := mapRole[role]; ok {
		return r, nil
	}
	return Role{}, fmt.Errorf("invalid role: %s", role)
}
