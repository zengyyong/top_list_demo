// 这个是我之前最早在我司自己工程中设计的双指针版本 这个更新性能更高 这个是第一次加载速度慢 由python修改来

package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type RankInfo struct {
	PlayerID  string `json:"playerId"`
	Score     int    `json:"score"`
	Rank      int    `json:"rank"`
	Timestamp int64  `json:"timestamp"` // 用于同分排序
}

type RankData struct {
	PlayerID  string
	Score     int
	Timestamp int64
}

type LeaderboardServiceImpl struct {
	CacheScoreRankDict map[string]RankData // 缓存更新
	ScoreRankDict      map[string]RankData // 所有玩家分数
	ScoreRankList      []RankData          // 官方排序列表 (前百万)
	N                  int                 // 最大排行大小，e.g., 1000000
	batchThreshold     int                 // 批量阈值，e.g., 10000
	lastRefresh        time.Time
}

func NewLeaderboardService(n int) *LeaderboardServiceImpl {
	return &LeaderboardServiceImpl{
		CacheScoreRankDict: make(map[string]RankData),
		ScoreRankDict:      make(map[string]RankData),
		ScoreRankList:      make([]RankData, 0),
		N:                  n,
		batchThreshold:     10000,
	}
}

func (s *LeaderboardServiceImpl) UpdateScore(playerID string, score int, timestamp int64) {
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	data := RankData{PlayerID: playerID, Score: score, Timestamp: timestamp}
	s.CacheScoreRankDict[playerID] = data

	s.refreshScoreRankList()
}

func (s *LeaderboardServiceImpl) refreshScoreRankList() {
	if time.Since(s.lastRefresh) < 2*time.Second {
		return
	}

	if len(s.CacheScoreRankDict) == 0 {
		return
	}

	rankCacheList := make([]RankData, 0, len(s.CacheScoreRankDict))
	for _, v := range s.CacheScoreRankDict {
		rankCacheList = append(rankCacheList, v)
	}
	sort.Slice(rankCacheList, func(i, j int) bool {
		if rankCacheList[i].Score == rankCacheList[j].Score {
			return rankCacheList[i].Timestamp < rankCacheList[j].Timestamp // 同分早时间优先
		}
		return rankCacheList[i].Score > rankCacheList[j].Score // 分数降序
	})

	rankOfficialList := s.ScoreRankList
	if len(rankOfficialList) > s.N {
		rankOfficialList = rankOfficialList[:s.N]
	}

	for k, v := range s.CacheScoreRankDict {
		s.ScoreRankDict[k] = v
	}

	rankList := s.refreshHelper(rankCacheList, rankOfficialList)
	if len(rankList) > s.N {
		s.ScoreRankList = rankList[:s.N]
		for _, rd := range rankList[s.N:] {
			delete(s.ScoreRankDict, rd.PlayerID)
		}
	} else {
		s.ScoreRankList = rankList
	}

	s.CacheScoreRankDict = make(map[string]RankData)
	s.lastRefresh = time.Now()
}

func (s *LeaderboardServiceImpl) refreshHelper(cache, official []RankData) []RankData {
	if len(cache) == 0 {
		return official
	} else if len(official) == 0 {
		return cache
	}

	rankList := make([]RankData, 0, len(cache)+len(official))
	cIdx, oIdx := 0, 0
	for cIdx < len(cache) && oIdx < len(official) {
		cScore, oScore := cache[cIdx].Score, official[oIdx].Score
		cTs, oTs := cache[cIdx].Timestamp, official[oIdx].Timestamp
		if cScore > oScore || (cScore == oScore && cTs < oTs) {
			rankList = append(rankList, cache[cIdx])
			cIdx++
		} else {
			rankList = append(rankList, official[oIdx])
			oIdx++
		}
	}
	// 剩余部分
	for ; oIdx < len(official); oIdx++ {
		rankList = append(rankList, official[oIdx])
	}
	for ; cIdx < len(cache); cIdx++ {
		rankList = append(rankList, cache[cIdx])
	}
	return rankList
}

func (s *LeaderboardServiceImpl) GetPlayerRank(playerID string) RankInfo {
	s.refreshScoreRankList()

	data, ok := s.ScoreRankDict[playerID]
	if !ok {
		return RankInfo{}
	}

	// 遍历已排好序的排行榜列表，找到玩家的排名
	for i, pData := range s.ScoreRankList {
		if pData.PlayerID == playerID {
			return RankInfo{
				PlayerID:  playerID,
				Score:     data.Score,
				Rank:      i + 1, // 索引 i + 1 就是玩家的排名
				Timestamp: data.Timestamp,
			}
		}
	}

	// 如果玩家不在 Top N 列表中，则返回排名为 0
	return RankInfo{
		PlayerID:  playerID,
		Score:     data.Score,
		Rank:      0,
		Timestamp: data.Timestamp,
	}
}

func (s *LeaderboardServiceImpl) GetTopN(n int) []RankInfo {
	s.refreshScoreRankList()

	if n > len(s.ScoreRankList) {
		n = len(s.ScoreRankList)
	}
	res := make([]RankInfo, n)
	for i := 0; i < n; i++ {
		d := s.ScoreRankList[i]
		res[i] = RankInfo{PlayerID: d.PlayerID, Score: d.Score, Rank: i + 1, Timestamp: d.Timestamp}
	}
	return res
}

func (s *LeaderboardServiceImpl) GetPlayerRankRange(playerID string, rangeN int) []RankInfo {
	s.refreshScoreRankList()

	data, ok := s.ScoreRankDict[playerID]
	if !ok {
		return nil
	}
	// 二分查找玩家位置
	idx := sort.Search(len(s.ScoreRankList), func(i int) bool {
		if s.ScoreRankList[i].Score == data.Score {
			return s.ScoreRankList[i].Timestamp >= data.Timestamp
		}
		return s.ScoreRankList[i].Score <= data.Score
	})
	start := idx - rangeN
	if start < 0 {
		start = 0
	}
	end := idx + rangeN + 1
	if end > len(s.ScoreRankList) {
		end = len(s.ScoreRankList)
	}
	res := make([]RankInfo, end-start)
	for i := start; i < end; i++ {
		d := s.ScoreRankList[i]
		res[i-start] = RankInfo{PlayerID: d.PlayerID, Score: d.Score, Rank: i + 1, Timestamp: d.Timestamp}
	}
	return res
}

func main() {
	size := 1000000
	totalSize := 2 * size
	singleSize := size / 10

	start := time.Now().UnixNano()
	lb := NewLeaderboardService(size)
	for i := 0; i < totalSize; i++ {
		lb.UpdateScore(fmt.Sprintf("player%d", i), i, time.Now().Unix())
	}
	end := time.Now().UnixNano()

	duration := (end - start) / 1e6
	fmt.Printf("插入 1200000 数据完成，耗时 %d ms\n", duration)

	time.Sleep(5 * time.Second)

	lb.refreshScoreRankList()

	time.Sleep(5 * time.Second)

	times := 100 // 测试 100 次
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < times; i++ {
		start := time.Now().UnixNano()
		count := r.Intn(singleSize)
		score := r.Intn(totalSize * 2)
		for i := 0; i < count; i++ {
			playId := r.Intn(totalSize)
			lb.UpdateScore(fmt.Sprintf("player%d", playId), score, time.Now().Unix())
		}

		top := lb.GetTopN(10)
		fmt.Println("Top 10:")
		for _, p := range top {
			fmt.Printf("Rank %d: %s, Score=%d\n", p.Rank, p.PlayerID, p.Score)
		}

		end := time.Now().UnixNano()
		duration := (end - start) / 1e6

		playId := r.Intn(totalSize)
		playerRank := lb.GetPlayerRank(fmt.Sprintf("player%d", playId))

		fmt.Printf("耗时 %d ms，更新条数 %d，随机玩家Id %d, 随机玩家信息 %+v\n\n", duration, count, playId, playerRank)

		time.Sleep(3 * time.Second)
	}
}
