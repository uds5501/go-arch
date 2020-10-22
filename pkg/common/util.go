package common

import (
	"database/sql"
	"fmt"
	//"trell/go-arch/redis"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"trell/go-arch/db"
	"trell/go-arch/logger"
	 "github.com/gomodule/redigo/redis"
	//"trell/go-arch/gocelery"
	"github.com/gocelery/gocelery"
)

type Util struct {
	dbFactory db.DBFactory
}

type Gratification struct{
	likes int
	comments int
	followers int
}

// for matrix elements
type actionRangeElement struct {
	likesRange [2]float64
	followersRange [2]float64
	extraFactor int
	minutesToBeSpent int
	commentsRange [2]float64
}

type followActionRangeElement struct {
	followersRange map[int][2]int
	followingRange map[int][2]int
	followerFollowingCondition int
}

type DelayedParamObject struct {
	TrailId int
	ViewsInitialValue int
	ViewsGlobalDelta int
	ViewsCurrentDelta int
	InitialTimestamp string
	FinalTimestamp string
	LikesInitialValue int
	LikesGlobalDelta int
	LikesCurrentDelta int
}

type Matrix struct{
	skewness map[int]int
	runtimeMatrix map[int]int
	textCountMatrix map[int]int
	actionRangeMatrix map[int]actionRangeElement
	followActionRangeMatrix map[int]followActionRangeElement
	titleLengthMatrix map[int]int
	weightageMatrix map[string]int
}

// DAO
func (u *Util) GetUserFollowers(userId int)  (int, error) {
	query := fmt.Sprintf("SELECT COUNT(followerId) FROM follower f WHERE friendId = %d AND status = 1", userId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var followerCount int
	for rows.Next() {
		err := rows.Scan(&followerCount)
		if err != nil {
			return -1, err
		}
	}
	return followerCount,err
}

// DAO
func (u *Util) GetTrailCounts(userId int) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(DISTINCT `trailListId`) AS trailCount FROM `userTrails` WHERE `userId` = %d AND `isDeleted` = 'false' AND `trailListId` IN (SELECT `trailListId` FROM `trailList` WHERE `isDeleted` = 'false')", userId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var trailCount int
	for rows.Next() {
		err := rows.Scan(&trailCount)
		if err != nil {
			return -1, err
		}
	}
	return trailCount, err
}

// DAO
func (u *Util) getUserFollowing(userId int) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(followerId) AS totalFollowing FROM follower f WHERE userId = %d AND status = 1", userId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var userFollowing int
	for rows.Next() {
		err := rows.Scan(&userFollowing)
		if err != nil {
			return -1, err
		}
	}
	return userFollowing, err
}

func (u *Util) UserQC(userIds []int, toRunForUserType int) {
	count := 0
	for _, userId := range userIds {
		count++
		params := make(map[string]int)
		trailCount, _ := u.GetTrailCounts(userId)
		params["trailCount"] = trailCount
		followerCount, _ := u.GetUserFollowers(userId)
		params["followerCount"] = followerCount
		followingCount, _ := u.getUserFollowing(userId)
		params["followingCount"] = followingCount
		requiredFollowers := u.findFollowersForContent(userId)
		params["requiredFollowers"] = requiredFollowers
		requiredFollowing := u.FindPalFollowing(userId)
		params["requiredFollowing"] = requiredFollowing
		requiredTrails := 1 - trailCount
		params["requiredTrails"] = requiredTrails
		userType := toRunForUserType
		params["userType"] = userType
		params["userId"] = userId
		fmt.Println(params)
		u.updatePalQC(params)
	}
}

// DAO
func (u *Util) updatePalQC(params map[string]int) int{
	queryPrefix := "INSERT into qcGratificationUsers21Feb20(userId, reqdFollowingCount, reqdFollowerCount, reqdTrailCount, followerCount, followingCount, trailCount, userType)"
	query := fmt.Sprintf("%s VALUES (%d,%d,%d,%d,%d,%d,%d,%d) on duplicate key update userId = userId", queryPrefix, params["userId"], params["requiredFollowing"], params["requiredFollowers"], params["requiredTrails"], params["followerCount"], params["followingCount"], params["trailCount"], params["userType"])
	fmt.Println(query)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	return 1
}

func (u *Util) ExecuteReadQuery(query string) *sql.Rows {
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		logger.Client().Error(err.Error())
	}
	return rows
}

// DAO
func (u *Util) GetTopmostUserId(trailListId int) (int, error) {
	query := fmt.Sprintf("select userId from userTrails where trailListId=%d LIMIT 1", trailListId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var userId int
	for rows.Next() {
		err := rows.Scan(&userId)
		if err != nil {
			return -1, err
		}
	}
	return userId,err
}

func (u *Util) GratificationForContent(trailListId int, t int, newFollowerCount int) map[string]int {
	var gratification map[string]int
	var metricDifference map[string]float64
	var gn map[string]float64
	gratification = make(map[string]int)
	metricDifference = make(map[string]float64)
	gn = make(map[string]float64)
	userId, _ := u.GetTopmostUserId(trailListId)
	followerCount, _ := u.GetUserFollowers(userId)

	if t == 1 {
		followerCount += newFollowerCount
	}

	matrix := u.GetActionRangeOnFollowers()
	gratificationMatrix := matrix.actionRangeMatrix
	gratificationMetric := u.GetActionValueInRange(gratificationMatrix, followerCount)
	qScore := u.QScoreGeneration(trailListId)
	gratification["qscore"] = int(qScore)
	metricDifference["likes"] = gratificationMetric.likesRange[1] - gratificationMetric.likesRange[0]
	metricDifference["followers"] = gratificationMetric.followersRange[1] - gratificationMetric.followersRange[0]
	metricDifference["comments"] = gratificationMetric.commentsRange[1] - gratificationMetric.commentsRange[0]
	gn["likes"] = metricDifference["likes"] * qScore
	gn["followers"] = metricDifference["followers"] * qScore
	gn["comments"] = metricDifference["comments"] * qScore

	if followerCount <= 100 {
		gratification["likes"] = int(gratificationMetric.likesRange[0] + gn["likes"])
		gratification["followers"] = int(gratificationMetric.followersRange[0] + gn["followers"])
		gratification["comments"] = int(gratificationMetric.commentsRange[0] + gn["comments"])
	} else if followerCount > 100 {
		fmt.Println("in > 100")
		isFeatured, _ := u.IsTrailFeature(trailListId)
		var randomNum float64
		if isFeatured == false {
			if gn["likes"] >= metricDifference["likes"] {
				randomNum = u.getRandomNumberInRange(1*1000, 2*1000) / 1000
			} else {
				randomNum = u.getRandomNumberInRange(0*1000, 1*1000) / 1000
			}
			gratification["likes"] = int(u.getRandomNumberInRange(int(gratificationMetric.likesRange[0])*1000, int(gratificationMetric.likesRange[1])*1000) / 100000 + randomNum*qScore*metricDifference["likes"])

			if gn["comments"] >= metricDifference["comments"] {
				randomNum = u.getRandomNumberInRange(1*1000, 2*1000) / 1000
			} else {
				randomNum = u.getRandomNumberInRange(0*1000, 1*1000) / 1000
			}
			gratification["likes"] = int(u.getRandomNumberInRange(int(gratificationMetric.commentsRange[0])*1000, int(gratificationMetric.commentsRange[1])*1000) / 100000 + randomNum*qScore*metricDifference["comments"])

			if gn["followers"] >= metricDifference["followers"] {
				randomNum = u.getRandomNumberInRange(1*1000, 2*1000) / 1000
			} else {
				randomNum = u.getRandomNumberInRange(0*1000, 1*1000) / 1000
			}
			gratification["likes"] = int(u.getRandomNumberInRange(int(gratificationMetric.followersRange[0])*1000, int(gratificationMetric.followersRange[1])*1000) / 100000 + randomNum*qScore*metricDifference["followers"])
		}
		if isFeatured == true {
			randomNum := u.getRandomNumberInRange(2*1000, 4*1000)/1000
			gratification["likes"] = int(u.getRandomNumberInRange(int(gratificationMetric.likesRange[0]*1000), int(gratificationMetric.likesRange[1]*1000)/100000 * followerCount) + randomNum*qScore*metricDifference["likes"])
			gratification["comments"] = int(u.getRandomNumberInRange(int(gratificationMetric.commentsRange[0]*1000), int(gratificationMetric.commentsRange[1]*1000)/100000 * followerCount) + randomNum*qScore*metricDifference["comments"])
			gratification["followers"] = int(u.getRandomNumberInRange(int(gratificationMetric.followersRange[0]*1000), int(gratificationMetric.followersRange[1]*1000)/100000 * followerCount) + randomNum*qScore*metricDifference["followers"])
		}
	}
	gratification["followerCount"] = followerCount
	return gratification
}


func (u *Util) AssignOnDemandFollowers(count int, userId int, minutes []int) int{
	followerIds,_ := u.GetUserFollowerIds(userId)
	glcData := make(map[string]int)
	glcData["glc"] = 1
	palIds :=  u.GetRandomPalIds(followerIds,count, true, glcData)
	fmt.Println(palIds) // random completion function for now
	cnt := u.Follow_pal_final(palIds, minutes, count, userId)
	fmt.Println(cnt)
	return 1
}

func (u *Util) AssignOnDemandLove(count int, trailId int, minutes []int) int {
	loverIds, _ := u.GetLoverIds(trailId)
	glcData := make(map[string]int)
	glcData["glc"] = 1
	palIds := u.GetRandomPalIds(loverIds, count, true, glcData)
	fmt.Println(palIds)
	call := u.LfvPalFinal(palIds, minutes, count, trailId)
	return call
}

func (u *Util) AssignOnDemandComment(count int, trailId int, minutes []int, comments map[string]string ) int {
	var empty []int
	glcData := u.GetGLCDataForTrail(trailId)

	palIds := u.GetRandomPalIds(empty, count, false, glcData)
	fmt.Println(palIds)
	call := u.ScheduleComments(palIds, minutes, comments, trailId)
	return call
}

// DAO
func (u *Util) GetGLCDataForTrail(trailId int) map[string]int {
	var glcData map[string]int
	glcData = make(map[string]int)
	glcData["glc"] = 2

	// get Category of Trail
	query := fmt.Sprintf("select categoryId from trailFeedCategories where trailId = %d limit 1", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
	}
	var category int
	for rows.Next() {
		err := rows.Scan(&category)
		if err != nil {
			break
		}
	}
	glcData["category"] = category

	// get gender of content creator
	genderQuery := fmt.Sprintf("select u.gender from users u inner join trailList on trailList.userId = u.userId where trailListId=%d", trailId)
	genderQuery = db.WrapQuery(genderQuery)
	rows, err = u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
	}
	var gender string
	var gs int

	for rows.Next() {
		err:= rows.Scan(&gender)
		if err != nil {
			break
		}
	}
	if gender == "male" {
		gs = 1
	} else if gender == "female" {
		gs = 2
	} else {
		gs = 3
	}
	glcData["gender"] = gs

	// get Language of trail
	query = fmt.Sprintf("Select userId, languageId from trailList where trailListId=%d", trailId)
	query = db.WrapQuery(query)
	rows, err = u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
	}
	var langId, userId  int
	for rows.Next() {
		err:= rows.Scan(&userId, &langId)
		if err != nil {
			break
		}
	}
	glcData["languageId"] = langId
	glcData["userId"] = userId

	return glcData
}

// Async Comment Push
func (u *Util) asyncCommentPush(t *time.Timer, duration int, trailId int, palId int, comment string) {
	<-t.C
	fmt.Printf("[EXECUTION]: Executing comment push after %d seconds\n", duration)
	query := fmt.Sprintf("INSERT INTO geoChatMessages (geoChatId, chatId, trailId, userId, userMessage, sourceId, a1) VALUES (0, 0, %d, %d, %s, 0, 0)", trailId, palId, comment)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	fmt.Printf("[EXECUTION]: Pal comment inserted for trail %d by %d\n", trailId, palId)
}

func (u *Util) ScheduleComments(palIds []int, hour []int, comments map[string]string, trailId int) int {
	for k:= range palIds{
		minute := rand.Intn(60)
		key := strconv.Itoa(k)
		curComment := fmt.Sprintf("'%s'", comments[key])
		duration := hour[k]*3600 + minute*60
		//duration := 20
		timer := time.NewTimer(time.Duration(duration)*time.Second)
		go u.asyncCommentPush(timer, duration, trailId, palIds[k], curComment)
		//query = fmt.Sprintf("%s %d, %d, NOW() + INTERVAL %d HOUR + INTERVAL %d MINUTE, 1, %s, 1000919),(", query, trailId, palIds[k], hour[k], minute, curComment)
	}
	return 1
}

// DAO
func (u *Util) GetLoverIds(trailId int) ([]int, error) {
	var lovers []int
	lovers = []int{}
	query := fmt.Sprintf("select userId from trailLove where trailId = %d and counter > 0", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return lovers, err
	}
	var tmpLoverId int
	for rows.Next() {
		err := rows.Scan(&tmpLoverId)
		lovers = append(lovers, tmpLoverId)
		if err != nil {
			return lovers, err
		}
	}
	//fmt.Println(lovers)
	return lovers, err
}

// DAO
func (u *Util) GetUserFollowerIds(userId int) ([]int,error){
	var followers []int
	query:= fmt.Sprintf("select userId from follower where friendId = %d and status = 1",userId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	//fmt.Println("one")
	//defer rows.Close()
	if err != nil {

		fmt.Println(err)
		return followers, err
	}
	//fmt.Println("two")
	var followerId int
	for rows.Next() {
		//fmt.Println(userId)
		// null needs to be handled separately than string
		err := rows.Scan(&followerId)
		followers = append(followers,followerId)
		if err != nil {
			return followers, err
		}
	}
	return followers,err
}

func (u *Util) arrayToString(a []int, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

// DAO
func (u *Util)  GetRandomPalIds(existingPals []int, count int, normal bool, glcConfig map[string]int) []int{
	if glcConfig["glc"] == 1 {
		var pals []int
		palIdString := u.arrayToString(existingPals, ",")
		query := "Select userId from profileRepo "
		if normal == true {
			query = fmt.Sprintf("%s where userId not in (%s)", query, palIdString)
		}
		query = fmt.Sprintf("%s LIMIT %d", query, count)
		//query:= fmt.Sprintf("select userId from profileRepo where userId not in(%s) LIMIT %d ",palIdString,count)
		query = db.WrapQuery(query)
		rows, err := u.dbFactory("reader").Query(query)
		if err != nil {
			fmt.Println(err)
			return pals
		}
		var userId int
		for rows.Next() {
			err := rows.Scan(&userId)
			if err != nil {
				log.Fatal(err)
			}
			log.Println(userId)
			pals = append(pals, userId)
		}
		return pals
	} else {
		var pals []int
		palIdString := u.arrayToString(existingPals, ",")

		// get followers first
		followerIds, _ := u.GetUserFollowerIds(glcConfig["userId"])
		followerIdString := u.arrayToString(followerIds, ",")

		query := "Select userId from profileRepo "
		q1 := ""
		if normal == true {
			query = fmt.Sprintf("%s where userId not in (%s)", query, palIdString)
		} else {
			q1 = fmt.Sprintf("%s where userId in (%s)", query, followerIdString)
		}
		//q1 := fmt.Sprintf("%s and userId in (%s)", query, followerIdString)
		qsGLC := ""
		if glcConfig["gender"] != 3 {
			qsGLC = fmt.Sprintf("%s and gender = %d", qsGLC, glcConfig["gender"])
		}
		qsGLC = fmt.Sprintf("%s and (languageOne=%d or languageTwo=%d)", qsGLC, glcConfig["languageId"], glcConfig["languageId"])
		qsGLC = fmt.Sprintf("%s and (CategoryOne=%d or CategoryTwo=%d or CategoryThree=%d)", qsGLC, glcConfig["category"], glcConfig["category"], glcConfig["category"])
		q1 = fmt.Sprintf("%s %s", q1, qsGLC)
		q1 = fmt.Sprintf("%s limit %d", q1, count)
		q1 = db.WrapQuery(q1)

		rows, err := u.dbFactory("reader").Query(q1)
		if err != nil {
			fmt.Println(err)
			return pals
		}
		var userId int
		for rows.Next() {
			err := rows.Scan(&userId)
			if err != nil {
				continue
			}
			pals = append(pals, userId)
		}
		// got all following bots and did the comments

		left := count - len(pals)
		if left > 0 {
			q2 := query
			qsGLC = ""
			if glcConfig["gender"] != 3 {
				qsGLC = fmt.Sprintf("%s where gender = %d", q2, glcConfig["gender"])
				qsGLC = fmt.Sprintf("%s and (languageOne=%d or languageTwo=%d)", qsGLC, glcConfig["languageId"], glcConfig["languageId"])
			} else {
				qsGLC = fmt.Sprintf("%s where (languageOne=%d or languageTwo=%d)", qsGLC, glcConfig["languageId"], glcConfig["languageId"])
			}
			qsGLC = fmt.Sprintf("%s and (CategoryOne=%d or CategoryTwo=%d or CategoryThree=%d)", qsGLC, glcConfig["category"], glcConfig["category"], glcConfig["category"])
			q2 = fmt.Sprintf("%s %s limit %d", q2, qsGLC, left)
			rows, err := u.dbFactory("reader").Query(q2)
			if err != nil {
				fmt.Println(err)
				return pals
			}
			var userId int
			for rows.Next() {
				err := rows.Scan(&userId)
				if err != nil {
					continue
				}
				pals = append(pals, userId)
			}
		}
		return pals
	}
}

// Async Pal Follow
func (u *Util) asyncPalFollow(t *time.Timer, duration int, palId int, userId int) {
	<-t.C
	fmt.Printf("[EXECUTION]: Executing after %d seconds\n", duration)
	query := fmt.Sprintf("INSERT INTO follower (userId, friendId) VALUES (%d, %d)",palId, userId)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	fmt.Printf("[EXECUTION]: Pal Follow inserted for trail %d \n", userId)
}

// DAO
func (u *Util) Follow_pal_final(palIds []int, hour []int, count int,userId int) int{
	for k := range palIds{
		randMin := rand.Intn(60)
		duration := hour[k]*3600 + randMin*60
		timer := time.NewTimer(time.Duration(duration)*time.Second)
		fmt.Printf("Pushing for %d \n", palIds[k])
		go u.asyncPalFollow(timer, duration, palIds[k], userId)
		//query = fmt.Sprintf("%s %d, %d,NOW() + INTERVAL %d HOUR + INTERVAL %d MINUTE,1),(",query,userId,palIds[k],hour[k],randMin)
	}
	return 1
}

// Async Pal Love
func (u *Util) asyncPalLove(t *time.Timer, duration int, trailId int, palId int) {
	<-t.C
	fmt.Printf("[EXECUTION]: Executing after %d seconds\n", duration)
	query := fmt.Sprintf("INSERT INTO trailLove (trailId, userId, counter) VALUES (%d, %d, %d)", trailId, palId, 1)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	fmt.Printf("[EXECUTION]: Pal Love inserted for trail %d \n", trailId)
}

// DAO
func (u *Util) LfvPalFinal(palIds []int, hour []int, count int, trailId int) int {
	for k:= range palIds {
		minute := rand.Intn(60)
		duration := hour[k]*3600 + minute
		timer := time.NewTimer(time.Duration(duration)*time.Second)
		fmt.Printf("Pushing for %d \n", palIds[k])
		go u.asyncPalLove(timer, duration, trailId, palIds[k])
	}
	return 1
}

func (u *Util) findNumberOfFollowersRequired(contentCount int) int {
	if contentCount >=0 && contentCount <= 5 {
		return 10*contentCount
	} else if contentCount >= 6 && contentCount <= 10 {
		return 15*contentCount
	} else if contentCount > 10 {
		return 20*contentCount
	}
	return 1
}

func (u *Util) findFollowersForContent(userId int) int {
	contentNumber, _ := u.GetTrailCounts(userId)
	followerCount, _ := u.GetUserFollowers(userId)
	newFollowers := u.findNumberOfFollowersRequired(contentNumber) - followerCount
	return newFollowers
}

func (u *Util) FindPalFollowing(userId int) int {
	followerCount, _ := u.GetUserFollowers(userId)
	minRange := 5
	maxRange := 10
	getRand := rand.Intn(maxRange - minRange) + minRange
	following := float64(getRand * followerCount)
	following = following/100
	return int(math.Floor(following))
}

func (u *Util) GetActionRangeOnFollowers() Matrix{
	var matrix Matrix
	matrix.skewness = map[int]int{
		100:4,
		1000:8,
		10000:16,
		100000: 32,
		1000000: 64,
		10000000: 128,
		100000000: 256,
		1000000000: 512,
	}
	matrix.runtimeMatrix = map[int]int{
		40: 0,
		60: 1,
		90: 2,
		120: 3,
		160: 4,
		180: 5,
		200: 6,
		215: 7,
		230: 8,
		239: 9,
		360: 10,
		370: 9,
		380: 8,
		390: 7,
		400: 6,
		410: 5,
		415: 4,
		420: 3,
		430: 2,
		480: 1,
		600: 0,
	}
	matrix.textCountMatrix = map[int]int {
		100: 0,
		200: 0,
		300: 1,
		400: 1,
		500: 2,
		600: 2,
		700: 3,
		800: 3,
		900: 4,
		1000: 4,
		1100: 5,
		1200: 5,
		1300: 6,
		1400: 6,
		1500: 7,
		1600: 7,
		1700: 8,
		1800: 8,
		1900: 8,
		2000: 10,
		2100: 10,
		2200: 10,
		2300: 10,
		2400: 10,
		2500: 10,
		2600: 10,
		2700: 10,
		2800: 10,
		2900: 10,
		3000: 10,
		3100: 8,
		3200: 8,
		3300: 8,
		3400: 7,
		3500: 7,
		3600: 7,
		3700: 5,
		3800: 5,
		3900: 5,
		4000: 5,
		4100: 5,
		4200: 3,
		4300: 3,
		4400: 3,
		4500: 2,
		4600: 2,
		4700: 2,
		4800: 1,
		4900: 1,
		5000: 1,
	}
	matrix.actionRangeMatrix = map[int]actionRangeElement{
		100: {
			likesRange: [2]float64{20.0, 150.0},
			followersRange: [2]float64{15.0, 30.0},
			extraFactor: 4,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{1.0, 10.0},
		},
		500: {
			likesRange: [2]float64{15.0, 110.0},
			followersRange: [2]float64{15.0, 15.0},
			extraFactor: 10,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.75, 3.0},
		},
		1000: {
			likesRange: [2]float64{20.0, 85},
			followersRange: [2]float64{3.0, 8.0},
			extraFactor: 12,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.75, 2.5},
		},
		2000: {
			likesRange: [2]float64{11.0, 65},
			followersRange: [2]float64{2.5, 7.0},
			extraFactor: 16,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.55, 2.7},
		},
		3000: {
			likesRange: [2]float64{11.0, 50},
			followersRange: [2]float64{2.5, 6.0},
			extraFactor: 20,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.55, 3.0},
		},
		4000: {
			likesRange: [2]float64{9.0, 40},
			followersRange: [2]float64{2.0, 5.0},
			extraFactor: 24,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.45, 3.1},
		},
		5000: {
			likesRange: [2]float64{8.0, 38},
			followersRange: [2]float64{1.80, 4.0},
			extraFactor: 28,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.4, 3.11},
		},
		6000: {
			likesRange: [2]float64{7.0, 37},
			followersRange: [2]float64{1.70, 3.80},
			extraFactor: 32,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.35, 3.2},
		},
		7000: {
			likesRange: [2]float64{6.5, 35},
			followersRange: [2]float64{1.50, 3.50},
			extraFactor: 36,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.325, 3.40},
		},
		8000: {
			likesRange: [2]float64{6.0, 32},
			followersRange: [2]float64{1.40, 3.0},
			extraFactor: 40,
			minutesToBeSpent: 4320,
			commentsRange: [2]float64{0.5, 3.5},
		},
		9000: {
			likesRange: [2]float64{5.8, 30},
			followersRange: [2]float64{1.30, 2.50},
			extraFactor: 44,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.29, 3.70},
		},
		10000: {
			likesRange: [2]float64{5.4, 29},
			followersRange: [2]float64{1.20, 2.0},
			extraFactor: 48,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.27, 4.0},
		},
		20000: {
			likesRange: [2]float64{4.0, 23},
			followersRange: [2]float64{1.0, 1.20},
			extraFactor: 90,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.2, 2.5},
		},
		30000: {
			likesRange: [2]float64{2.2, 17},
			followersRange: [2]float64{0.50, 0.80},
			extraFactor: 130,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.11, 1.90},
		},
		40000: {
			likesRange: [2]float64{1.8, 16},
			followersRange: [2]float64{0.3, 0.7},
			extraFactor: 155,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.09, 1.5},
		},
		50000: {
			likesRange: [2]float64{1.5, 15},
			followersRange: [2]float64{0.20, 0.60},
			extraFactor: 170,
			minutesToBeSpent: 5760,
			commentsRange: [2]float64{0.075, 1.30},
		},
		60000: {
			likesRange: [2]float64{1.4, 14},
			followersRange: [2]float64{0.1, 0.5},
			extraFactor: 170,
			minutesToBeSpent: 7200,
			commentsRange: [2]float64{0.07, 1.2},
		},
		70000: {
			likesRange: [2]float64{1.3, 13},
			followersRange: [2]float64{0.10, 0.40},
			extraFactor: 170,
			minutesToBeSpent: 7200,
			commentsRange: [2]float64{0.065, 1.20},
		},
		80000: {
			likesRange: [2]float64{1.2, 12},
			followersRange: [2]float64{0.1, 0.3},
			extraFactor: 170,
			minutesToBeSpent: 8640,
			commentsRange: [2]float64{0.06, 1.2},
		},
		90000: {
			likesRange: [2]float64{1.1, 11},
			followersRange: [2]float64{0.10, 0.30},
			extraFactor: 170,
			minutesToBeSpent: 8640,
			commentsRange: [2]float64{0.055, 1.10},
		},
		100000: {
			likesRange: [2]float64{1.0, 10},
			followersRange: [2]float64{0.1, 0.4},
			extraFactor: 170,
			minutesToBeSpent: 8640,
			commentsRange: [2]float64{0.05, 1.0},
		},
		1000000: {
			likesRange: [2]float64{0.95, 3},
			followersRange: [2]float64{0.10, 0.20},
			extraFactor: 170,
			minutesToBeSpent: 10080,
			commentsRange: [2]float64{0.0475, 0.10},
		},
		10000000: {
			likesRange: [2]float64{0.1, 1},
			followersRange: [2]float64{0.10, 0.20},
			extraFactor: 340,
			minutesToBeSpent: 10080,
			commentsRange: [2]float64{0.005, 0.05},
		},
	}
	matrix.followActionRangeMatrix = map[int]followActionRangeElement{
		0: {
			followersRange: map[int][2]int{
				40: [2]int{12, 60},
				30: [2]int{61, 90},
				20: [2]int{91, 150},
				10: [2]int{151, 180},
			},
			followingRange: map[int][2]int{
				40: [2]int{80, 180},
				30: [2]int{181, 250},
				20: [2]int{251, 350},
				10: [2]int{351, 380},
			},
			followerFollowingCondition: 0,
		},
		5: {
			followersRange: map[int][2]int{
				40: [2]int{62, 110},
				30: [2]int{111, 140},
				20: [2]int{141, 200},
				10: [2]int{201, 230},
			},
			followingRange: map[int][2]int{
				40: [2]int{80, 180},
				30: [2]int{181, 250},
				20: [2]int{251, 350},
				10: [2]int{351, 380},
			},
			followerFollowingCondition: -1,
		},
		1000000: {
			followersRange: map[int][2]int{
				40: [2]int{62, 110},
				30: [2]int{111, 140},
				20: [2]int{141, 200},
				10: [2]int{201, 230},
			},
			followingRange: map[int][2]int{
				40: [2]int{80, 180},
				30: [2]int{181, 250},
				20: [2]int{251, 350},
				10: [2]int{351, 380},
			},
			followerFollowingCondition: 1,
		},
	}
	matrix.titleLengthMatrix = map[int]int{
		1: 0,
		2: 0,
		3: 0,
		4: 0,
		5: 2,
		6: 2,
		7: 2,
		8: 3,
		9: 3,
		10: 5,
		11: 5,
		12: 5,
		13: 5,
		14: 5,
		15: 5,
		16: 10,
		17: 10,
		18: 10,
		19: 10,
		20: 10,
		21: 10,
		22: 10,
		23: 10,
		24: 10,
		25: 10,
		26: 10,
		27: 10,
		28: 10,
		29: 10,
		30: 10,
		31: 10,
		32: 10,
		33: 10,
		34: 10,
		35: 10,
		36: 10,
		37: 10,
		38: 10,
		39: 10,
		40: 10,
		41: 5,
		42: 5,
		43: 5,
		44: 5,
		45: 5,
		46: 5,
		47: 5,
		48: 5,
		49: 5,
		50: 5,
		51: 3,
		52: 3,
		53: 3,
		54: 3,
		55: 3,
		56: 1,
		57: 1,
		58: 1,
		59: 1,
		60: 1,
		61: 1,
		62: 1,
		63: 1,
		64: 1,
		65: 1,
		66: 1,
		67: 1,
		68: 1,
		69: 1,
		70: 1,
		71: 0,
		72: 0,
		73: 0,
		74: 0,
		75: 0,
		76: 0,
		77: 0,
		78: 0,
		79: 0,
		80: 0,
		81: 0,
		82: 0,
		83: 0,
		84: 0,
		85: 0,
		86: 0,
		87: 0,
		88: 0,
		89: 0,
		90: 0,
		91: 0,
		92: 0,
		93: 0,
		94: 0,
		95: 0,
		96: 0,
		97: 0,
		98: 0,
		99: 0,
		100: 0,
		101: 0,
		102: 0,
		103: 0,
		104: 0,
		105: 0,
		106: 0,
		107: 0,
		108: 0,
		109: 0,
		110: 0,
		111: 0,
		112: 0,
		113: 0,
		114: 0,
		115: 0,
		116: 0,
		117: 0,
		118: 0,
		119: 0,
		120: 0,
		121: 0,
		122: 0,
		123: 0,
		124: 0,
		125: 0,
		126: 0,
		127: 0,
		128: 0,
		129: 0,
		130: 0,
		131: 0,
		132: 0,
		133: 0,
		134: 0,
		135: 0,
		136: 0,
		137: 0,
		138: 0,
		139: 0,
		140: 0,
		141: 0,
		142: 0,
		143: 0,
		144: 0,
		145: 0,
		146: 0,
		147: 0,
		148: 0,
		149: 0,
		150: 0,
		151: 0,
		152: 0,
		153: 0,
		154: 0,
		155: 0,
		156: 0,
		157: 0,
		158: 0,
		159: 0,
		160: 0,
		161: 0,
		162: 0,
		163: 0,
		164: 0,
		165: 0,
		166: 0,
		167: 0,
		168: 0,
		169: 0,
		170: 0,
		171: 0,
		172: 0,
		173: 0,
		174: 0,
		175: 0,
		176: 0,
		177: 0,
		178: 0,
		179: 0,
		180: 0,
		181: 0,
		182: 0,
		183: 0,
		184: 0,
		185: 0,
		186: 0,
		187: 0,
		188: 0,
		189: 0,
		190: 0,
		191: 0,
		192: 0,
		193: 0,
		194: 0,
		195: 0,
		196: 0,
		197: 0,
		198: 0,
		199: 0,
		200: 0,
	}
	matrix.weightageMatrix = map[string]int{
		"runtime": 50,
		"titleLength": 20,
		"aspectRatio": 0,
		"thumbnail": 30,
	}
	return matrix
}

// DAO
func (u *Util) GetTrailVideoDuration(trailId int)  (int, error) {

	query:= fmt.Sprintf("SELECT `duration` FROM `trailVideoStatus` WHERE `trailId` = %d AND `active` = 1",trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	//fmt.Println("one")
	//defer rows.Close()
	if err != nil {

		fmt.Println(err)
		return -1, err
	}
	//fmt.Println("two")
	var duration int
	for rows.Next() {
		var nullDuration sql.NullString
		//fmt.Println(userId)
		// null needs to be handled separately than string
		err := rows.Scan(&nullDuration)
		if nullDuration.Valid{
			temp :=nullDuration.String
			duration,_= strconv.Atoi(temp)
		}
		if err != nil {
			return -1, err
		}
	}
	return duration,err
}

//DAO
func (u *Util) GetTrailTitleLength(trailId int)  (int, error) {

	query:= fmt.Sprintf("SELECT `duration` FROM `trailVideoStatus` WHERE `trailId` = %d AND `active` = 1",trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	//fmt.Println("one")
	//defer rows.Close()
	if err != nil {

		fmt.Println(err)
		return -1, err
	}
	//fmt.Println("two")
	var duration int
	for rows.Next() {
		//fmt.Println(userId)
		// null needs to be handled separately than string
		err := rows.Scan(&duration)
		if err != nil {
			return -1, err
		}
	}
	return duration,err
}

func (u *Util) GetMatrixValueInRange(mat map[int]int, target int) int {
	lastTaken := 10000000005
	ans := -1
	for k := range mat {
		if target <= k {
			if k < lastTaken {
				lastTaken = k
				ans = mat[k]
			}
		}
	}
	return ans
}

func (u *Util) GratificationDistributionQuotient(time int, followerCount int, total int) float64 {
	//matrix := u.GetActionRangeOnFollowers()
	//skewnessMatrix := matrix.skewness
	//skQuotient := u.GetMatrixValueInRange(skewnessMatrix, followerCount)
	skQuotient := 8
	ex := float64(-1 * time)/float64(2*skQuotient)
	num := (float64(time)/2) * (math.Exp(ex))
	deno := math.Pow(float64(skQuotient), 2.0)
	gq := (float64(total)*num) / deno
	return gq
}

func (u *Util) GratificationVolume(trailId int, followerCount int) map[string]float64 {
	matrix := u.GetActionRangeOnFollowers()
	actionMatrix := matrix.actionRangeMatrix
	applicableActionRange := u.GetActionValueInRange(actionMatrix, followerCount)

	minLikes := applicableActionRange.likesRange[0]
	maxLikes := applicableActionRange.likesRange[1]

	minFollowers := applicableActionRange.followersRange[0]
	maxFollowers := applicableActionRange.followersRange[1]

	minComments := applicableActionRange.commentsRange[0]
	maxComments := applicableActionRange.commentsRange[1]

	trailQualityData, _ := u.GetTrailQuality(trailId)
	trailQuality := float64(trailQualityData)
	var gratificationVolume map[string]float64
	gratificationVolume = make(map[string]float64)
	gratificationVolume["likes"] = (minLikes + trailQuality*(maxLikes - minLikes)) * 0.01 * float64(followerCount)
	gratificationVolume["followers"] = (minFollowers + trailQuality*(maxFollowers - minFollowers)) * 0.01 * float64(followerCount)
	gratificationVolume["comments"] = (minComments + trailQuality*(maxComments - minComments)) * 0.01 * float64(followerCount)

	return gratificationVolume
}

func (u *Util) GetActionValueInRange(mat map[int]actionRangeElement, target int) actionRangeElement {
	lastTaken := 10000000005
	var ans actionRangeElement
	for k := range mat {
		if target <= k {
			if k < lastTaken {
				lastTaken = k
				ans = mat[k]
			}
		}
	}
	return ans
}

// DAO
func (u *Util) GetTrailQuality(trailId int) (int, error) {
	query := fmt.Sprintf("select trail_quality from trailQualityScore where trailId = %d LIMIT 1", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var trailQuality int
	for rows.Next() {
		err := rows.Scan(&trailQuality)
		if err != nil {
			return -1, err
		}
	}
	return trailQuality,err
}

func (u *Util) BotContentGratification(trailListId int) map[string]float64 {
	userId, _ := u.GetUserIdwWthTrailList(trailListId)
	followerCount, _ := u.GetUserFollowersCount(userId)
	isTrailFeatured, _ := u.IsTrailFeature(trailListId)
	var gratification map[string]float64
	gratification = make(map[string]float64)
	if isTrailFeatured == true{
		num := u.getRandomNumberInRange(5*100, 10*100) / 100.0
		gratification["views"] = math.Floor(float64(followerCount)*num)
	} else {
		num := u.getRandomNumberInRange(1*100, 3*100) / 100.0
		gratification["views"] = math.Floor(float64(followerCount)*num)
	}
	gratification["likes"] = math.Floor(gratification["views"]*u.getRandomNumberInRange(0*100, 1*100) / 100.0)
	gratification["comments"] = math.Floor(gratification["likes"]*u.getRandomNumberInRange(0*100, 1*100) / 100.0)
	gratification["shares"] = math.Floor(gratification["comments"]*u.getRandomNumberInRange(0*100, 1*100) / 100.0)
	return gratification
}

// DAO
func (u* Util) GetUserIdwWthTrailList(trailListId int) (int, error) {
	query := fmt.Sprintf("select userId from userTrails where trailListId= %d LIMIT 1", trailListId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var result int
	for rows.Next() {
		err := rows.Scan(&result)
		if err != nil {
			return -1, err
		}
	}
	return result,err
}

// DAO
func (u *Util) GetUserFollowersCount(userId int) (int, error) {
	query := fmt.Sprintf("select followers_count from users where userId = %d", userId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var result int
	for rows.Next() {
		err := rows.Scan(&result)
		if err != nil {
			return -1, err
		}
	}
	return result,err
}

//DAO
func (u *Util) IsTrailFeature(trailId int) (bool, error) {
	query := fmt.Sprintf("select categoryId from trailFeedCategories where trailId = %d limit 1", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	var category int
	for rows.Next() {
		err := rows.Scan(&category)
		if err != nil {
			return false, err
		}
	}
	if category == 7{
		return true, err
	}
	return false,err
}

func (u *Util) getRandomNumberInRange(mini int, maxi int) float64 {
	fmt.Println(mini, maxi)
	// [a, b]
	var el int
	if maxi-mini != 0 {
		el = rand.Intn(maxi-mini) + mini // (0 -> n) | mt_rand
	} else {
		el = mini
	}
	fmt.Println("omk")
	return float64(el)
}

func (u *Util) QScoreGeneration(trailId int) float64 {
	runtime, _ := u.GetTrailVideoDurationViaGeochat(trailId)
	titleLength, _ := u.GetTrailTitleLengthQScore(trailId)
	thumbnail := 1
	qScore := 0
	matrix := u.GetActionRangeOnFollowers()
	// runtime, title, weightage
	runtimeM := matrix.runtimeMatrix
	titleM := matrix.titleLengthMatrix
	weights := matrix.weightageMatrix

	if thumbnail == 1{
		qScore = qScore + 10*weights["thumbnail"]
	}
	runtimeMetric := u.GetMatrixValueInRange(runtimeM, runtime)
	qScore = qScore + runtimeMetric*weights["runtime"]
	if titleLength <= 200 {
		qScore = qScore + weights["titleLength"]*titleM[titleLength]
	}
	return float64(qScore) / 1000.0
}

// DAO
func (u *Util) GetTrailVideoDurationViaGeochat(trailId int) (int, error) {
	query := fmt.Sprintf("select ut.trailListId trailId, sum(gc.duration) durationSum from userTrails ut inner join geoChat gc on ut.geoChatId = gc.geoChatId where ut.trailListId = %d", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	var duration, trailIdIn int
	for rows.Next() {
		err := rows.Scan(&trailIdIn, &duration)
		if err != nil {
			return 0, err
		}
	}
	return duration,err
}

//DAO
func(u *Util) GetTrailTitleLengthQScore(trailId int) (int, error) {
	query := fmt.Sprintf("select name from trailList where trailListId = %d", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	var st int
	for rows.Next() {
		err := rows.Scan(&st)
		if err != nil {
			return 0, err
		}
	}
	return st,err
}

// DAO
func(u *Util) UpdateTrailViews(trailId int, newViewCount int, isDelta bool, incrementOnly bool) {
	sql := fmt.Sprintf("UPDATE trailList SET views = %d WHERE trailListId = %d", newViewCount, trailId)
	if isDelta == true {
		sql = fmt.Sprintf("UPDATE trailList SET views = (views + (%d)) WHERE trailListId = %d", newViewCount, trailId)
		if incrementOnly == true && newViewCount < 0 {
			return
		}
	} else if incrementOnly == true {
		currentViewCount, _ := u.GetTrailViews(trailId)
		if newViewCount < currentViewCount {
			return
		}
	}
	sql = db.WrapQuery(sql)
	u.dbFactory("writer").Query(sql)
}

// DAO
func(u *Util) GetTrailViews(trailId int) (int, error){
	query := fmt.Sprintf("select views from trailList where trailListId = %d", trailId)
	query = db.WrapQuery(query)
	rows, err := u.dbFactory("reader").Query(query)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var cnt int
	for rows.Next() {
		err := rows.Scan(&cnt)
		if err != nil {
			return -1, err
		}
	}
	return cnt,err
}

//DAO
func(u *Util) UpdateDelayedTrailInteractionMapViews(params DelayedParamObject) int {
	query := fmt.Sprintf("INSERT INTO delayedTrailInteractionMap(trailId,viewsInitialValue,viewsGlobalDelta,viewsCurrentDelta,initialTimestamp,finalTimestamp) VALUES (%d,%d,%d,%d,'%s','%s') on duplicate key update viewsInitialValue = %d, viewsGlobalDelta = %d, viewsCurrentDelta = %d,initialTimestamp= '%s', finalTimestamp  = '%s'",
		params.TrailId, params.ViewsInitialValue, params.ViewsGlobalDelta, params.ViewsCurrentDelta, params.InitialTimestamp, params.FinalTimestamp, params.ViewsInitialValue, params.ViewsGlobalDelta, params.ViewsCurrentDelta, params.InitialTimestamp, params.FinalTimestamp)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	return 1
}

//DAO
func (u *Util) TrailLoveCount(trailId int) (int, error) {
	sql := fmt.Sprintf("SELECT SUM(counter) AS total FROM trailLove WHERE trailId = %d", trailId)
	sql = db.WrapQuery(sql)
	rows, err := u.dbFactory("reader").Query(sql)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	var cnt int
	for rows.Next() {
		err := rows.Scan(&cnt)
		if err != nil {
			return -1, err
		}
	}
	return cnt,err
}

//DAO
func (u *Util) UpdateDelayedTrailInteractionMapLove(params DelayedParamObject) int {
	query := fmt.Sprintf("INSERT INTO delayedTrailInteractionMap(trailId,likesInitialValue,likesGlobalDelta,likesCurrentDelta,initialTimestamp,finalTimestamp) VALUES (%d,%d,%d,%d,'%s','%s') on duplicate key update likesInitialValue = %d, likesGlobalDelta = %d, likesCurrentDelta = %d,initialTimestamp= '%s', finalTimestamp  = '%s",
		params.TrailId, params.LikesInitialValue, params.LikesGlobalDelta, params.LikesCurrentDelta, params.InitialTimestamp, params.FinalTimestamp, params.LikesInitialValue, params.LikesGlobalDelta, params.LikesCurrentDelta, params.InitialTimestamp, params.FinalTimestamp)
	query = db.WrapQuery(query)
	u.dbFactory("writer").Query(query)
	return 1
}

func NewUtil(factory db.DBFactory) *Util {
	return &Util{dbFactory: factory}
}


func (u *Util) CeleryClientTest()  () {

	// create redis connection pool
	redisPool := &redis.Pool{
		MaxIdle:     3,                 // maximum number of idle connections in the pool
		MaxActive:   0,                 // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 240 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(redisPool),
		&gocelery.RedisCeleryBackend{Pool: redisPool},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)


	//fmt.Println("taskName %s, Result %d",taskName,22)
	// run task
	asyncResult, err := cli.Delay(taskName, argA, argB)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err := asyncResult.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type ", res)
}




