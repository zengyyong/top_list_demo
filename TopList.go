package main

import (
    "container/heap"
    "fmt"
    "math/rand"
    "sort"
    "time"
)

type PlayerScore struct {
    PlayerID  string
    Score     int
    Timestamp int64
}

type MinHeap []*PlayerScore

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
    if h[i].Score == h[j].Score {
        return h[i].Timestamp < h[j].Timestamp
    }
    return h[i].Score < h[j].Score
}
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) {
    *h = append(*h, x.(*PlayerScore))
}
func (h *MinHeap) Pop() interface{} {
    old := *h
    n := len(old)
    x := old[n-1]
    *h = old[:n-1]
    return x
}

type Leaderboard struct {
    N           int
    heap        MinHeap
    playerMap   map[string]*PlayerScore
    heapMap     map[string]bool
    sortedList  []*PlayerScore
    rankIndex   map[string]int
    lastUpdate  time.Time
    needRefresh bool
}

func NewLeaderboard(N int) *Leaderboard {
    h := &MinHeap{}
    heap.Init(h)
    return &Leaderboard{
        N:           N,
        heap:        *h,
        playerMap:   make(map[string]*PlayerScore),
        heapMap:     make(map[string]bool),
        rankIndex:   make(map[string]int),
        lastUpdate:  time.Now(),
        needRefresh: true,
    }
}

func (lb *Leaderboard) UpdateScore(playerID string, score int, timestamp int64) {
    ps := &PlayerScore{playerID, score, timestamp}
    lb.playerMap[playerID] = ps

    if lb.heapMap[playerID] {
        lb.needRefresh = true
    } else if lb.heap.Len() < lb.N {
        heap.Push(&lb.heap, ps)
        lb.heapMap[playerID] = true
        lb.needRefresh = true
    } else if score > lb.heap[0].Score || (score == lb.heap[0].Score && timestamp > lb.heap[0].Timestamp) {
        removed := heap.Pop(&lb.heap).(*PlayerScore)
        delete(lb.heapMap, removed.PlayerID)
        heap.Push(&lb.heap, ps)
        lb.heapMap[playerID] = true
        lb.needRefresh = true
    }
}

func (lb *Leaderboard) refreshSortedList() {
    if !lb.needRefresh && time.Since(lb.lastUpdate) < 10*time.Second {
        return
    }
    lb.sortedList = make([]*PlayerScore, lb.heap.Len())
    copy(lb.sortedList, lb.heap)
    sort.Slice(lb.sortedList, func(i, j int) bool {
        if lb.sortedList[i].Score == lb.sortedList[j].Score {
            return lb.sortedList[i].Timestamp < lb.sortedList[j].Timestamp
        }
        return lb.sortedList[i].Score > lb.sortedList[j].Score
    })
    lb.rankIndex = make(map[string]int)
    rank := 1
    for i, ps := range lb.sortedList {
        if i > 0 && lb.sortedList[i].Score == lb.sortedList[i-1].Score {
            lb.rankIndex[ps.PlayerID] = rank
        } else {
            rank = i + 1
            lb.rankIndex[ps.PlayerID] = rank
        }
    }
    lb.lastUpdate = time.Now()
    lb.needRefresh = false
}

func (lb *Leaderboard) GetPlayerRank(playerID string) *PlayerScore {
    lb.refreshSortedList()
    if _, ok := lb.rankIndex[playerID]; !ok {
        return nil
    }
    ps := lb.playerMap[playerID]
    return &PlayerScore{ps.PlayerID, ps.Score, int64(lb.rankIndex[playerID])}
}

func (lb *Leaderboard) GetTopN(n int) []*PlayerScore {
    lb.refreshSortedList()
    if n > len(lb.sortedList) {
        n = len(lb.sortedList)
    }
    res := make([]*PlayerScore, n)
    for i := 0; i < n; i++ {
        ps := lb.sortedList[i]
        res[i] = &PlayerScore{ps.PlayerID, ps.Score, int64(lb.rankIndex[ps.PlayerID])}
    }
    return res
}

func (lb *Leaderboard) GetPlayerRankRange(playerID string, rangeCount int) []*PlayerScore {
    lb.refreshSortedList()
    rank, ok := lb.rankIndex[playerID]
    if !ok {
        return nil
    }
    start := rank - rangeCount - 1
    if start < 0 {
        start = 0
    }
    end := rank + rangeCount
    if end > len(lb.sortedList) {
        end = len(lb.sortedList)
    }
    res := []*PlayerScore{}
    for i := start; i < end; i++ {
        ps := lb.sortedList[i]
        res = append(res, &PlayerScore{ps.PlayerID, ps.Score, int64(lb.rankIndex[ps.PlayerID])})
    }
    return res
}

func main() {

    start := time.Now().UnixNano()
    lb := NewLeaderboard(1000000)
    for i := 0; i < 1200000; i++ {
        lb.UpdateScore(fmt.Sprintf("player%d", i), i%500, time.Now().Unix())
    }
    end := time.Now().UnixNano()

    duration := (end - start) / 1e6
    fmt.Printf("插入 1200000 数据完成，耗时", duration)

    time.Sleep(10 * time.Second)

    times := 1000000
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    for i := 0; i < times; i++ {
        start := time.Now().UnixNano()

        count := r.Intn(10000)
        score := r.Intn(10000000)
        for i := 0; i < count; i++ {
            playId := r.Intn(1500000)
            lb.UpdateScore(fmt.Sprintf("player%d", playId), score, time.Now().Unix())
        }

        top := lb.GetTopN(10)
        for _, p := range top {
            fmt.Println(p.PlayerID, p.Score, p.Timestamp) // Timestamp 即 rank
        }

        end := time.Now().UnixNano()

        playId := r.Intn(1500000)
        playerRank := lb.GetPlayerRank(fmt.Sprintf("player%d", playId))

        duration := (end - start) / 1e6

        fmt.Printf("耗时， 信息", count, duration, playerRank)

        time.Sleep(2 * time.Second)
    }
}
