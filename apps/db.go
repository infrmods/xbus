package apps

import (
	"time"
)

type App struct {
	Id         int64     `json:"-"`
	Name       string    `json:"name"`
	Desc       string    `json:"desc,omitempty"`
	Secret     string    `json:"-"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

type Group struct {
	Id         int64     `json:"-"`
	Name       string    `json:"name"`
	Desc       string    `json:"desc,omitempty"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

type GroupLink struct {
	Id      int64
	AppId   int64
	GroupId int64
}

type Perm struct {
	Id         int64
	PermType   int
	TargetType int
	TargetId   int64
	Content    string
}
