package OMongo

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"log"
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"runtime"
	"github.com/sd653159/OMongo/O"
	"strings"
	)

var debug = true
//调试模式开关
func SetDebug(flag bool) {
	debug = flag
}

//调试打印
func Oprintf(format string, a ...interface{}) {
	if debug {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf(fmt.Sprintf("[debug:%v:%v]%v\n", file, line, format), a...)
	}
}

//初始化数据库
func InitMongo(mongoAddrees string, dbName string) *mgo.Database {
	//数据库默认地址
	if mongoAddrees == "" {
		mongoAddrees = "127.0.0.1:27017"
	}
	Oprintf("mongoAddrees:%v", mongoAddrees)

	//数据库名字
	if dbName == "" {
		log.Fatal("dbName not found")
	}
	Oprintf("dbName:%v", dbName)

	//连接数据库服务器
	session, err := mgo.Dial(fmt.Sprintf("%v", mongoAddrees))

	if err != nil {
		Oprintf("%v", err)
	}
	session.SetMode(mgo.Monotonic, true)

	//返回数据库操作句柄
	dbHandler := session.DB(dbName)

	return dbHandler
}



//插入数据
func Insert(col *mgo.Collection, data interface{}) {
	var err error
	switch data.(type) {
	case bson.M:
		err = col.Insert(data)
	case map[string]interface{}:
		err = col.Insert(data)
	case O.M:
		err = col.Insert(data)
	case string:
		var td bson.M
		err = json.Unmarshal([]byte(data.(string)), &td)
		if err != nil {
			Oprintf("%v", err)
		}
		err = col.Insert(td)
	}
	if err != nil {
		Oprintf("%v", err)
	}
}

//删除数据
func Remove(col *mgo.Collection, data interface{}) {
	var err error
	switch data.(type) {
	case bson.M:
		err = col.Remove(data)
	case O.M:
		err = col.Insert(data)
	case map[string]interface{}:
		err = col.Remove(data)
	case string:
		var td bson.M
		err = json.Unmarshal([]byte(data.(string)), &td)
		if err != nil {
			Oprintf("%v", err)
		}
		err = col.Remove(td)
	}

	if err != nil {
		Oprintf("%v", err)
	}
}

//根据ID删除数据
func RemoveID(col *mgo.Collection, id string) {
	err := col.RemoveId(bson.ObjectIdHex(id))
	if err != nil {
		Oprintf("%v", err)
	}
}

//更新整个文档
func Update(col *mgo.Collection, cdi interface{}, data interface{}, part bool) {

	var err error
	switch cdi.(type) {
	case string:
		var tCdi = make(O.M)
		err = json.Unmarshal([]byte(cdi.(string)), &tCdi)
		cdi = tCdi
		if err != nil {
			Oprintf("%v", err)
		}
	}
	//支持直接id筛选
	if cdi.(O.M)["_id"] != nil {
		cdi.(O.M)["_id"] = bson.ObjectIdHex(cdi.(O.M)["_id"].(string))
	}

	switch data.(type) {
	case bson.M:
		if part {
			err = col.Update(cdi, bson.M{"$set": data,})
		} else {
			err = col.Update(cdi, data)
		}
	case O.M:
		if part {
			err = col.Update(cdi, O.M{"$set": data,})
		} else {
			err = col.Update(cdi, data)
		}
	case map[string]interface{}:
		if part {
			err = col.Update(cdi, O.M{"$set": data,})
		} else {
			err = col.Update(cdi, data)
		}
	case string:
		var td bson.M
		err = json.Unmarshal([]byte(data.(string)), &td)
		if err != nil {
			Oprintf("%v", err)
		}
		if part {
			err = col.Update(cdi, O.M{"$set": td,})
		} else {
			err = col.Update(cdi, td)
		}
	}
	if err != nil {
		Oprintf("%v", err)
	}
}

//更新文档的List内容
func UpdateList(col *mgo.Collection, cdi interface{}, data interface{}, distinct bool) {
	var com string
	if distinct {
		com = "$addToSet"
	} else {
		com = "$push"
	}

	var err error
	switch cdi.(type) {
	case string:
		var tCdi = make(O.M)
		err = json.Unmarshal([]byte(cdi.(string)), &tCdi)
		cdi = tCdi
		if err != nil {
			Oprintf("%v", err)
		}
	}
	//支持直接id筛选
	if cdi.(O.M)["_id"] != nil {
		cdi.(O.M)["_id"] = bson.ObjectIdHex(cdi.(O.M)["_id"].(string))
	}

	switch data.(type) {
	case bson.M:
		err = col.Update(cdi, O.M{com: data})
	case O.M:
		err = col.Update(cdi, O.M{com: data})
	case map[string]interface{}:
		err = col.Update(cdi, O.M{com: data})
	case string:
		var td bson.M
		err = json.Unmarshal([]byte(data.(string)), &td)
		if err != nil {
			Oprintf("%v", err)
		}
		err = col.Update(cdi, O.M{com: td})
	}
	if err != nil {
		Oprintf("%v", err)
	}
}

//查询数据
func Find(col *mgo.Collection, cdi interface{}, options interface{}) interface{}{
	//直接支持id筛选
	if cdi.(O.M)["_id"] != nil {
		cdi.(O.M)["_id"] = bson.ObjectIdHex(cdi.(O.M)["_id"].(string))
	}

	var result  []interface{}
	col.Find(cdi).All(&result)
	Oprintf("%v",result)

	itemList := strings.Split(options.(string),".")
	Oprintf("%v",itemList)
	var oData = result[0]
	for _,v := range itemList{
		oData = oData.(bson.M)[v]
		Oprintf("%v:%v",oData,v)
	}


	return result
}