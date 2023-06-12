//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    return false;
  }
  bool seen_less_than_k = false;
  bool seen_evictable = false;
  frame_id_t candidate;
  size_t min_ts;
  for (auto &[id, cur_frame] : node_store_) {
    if (!cur_frame.is_evictable_) {
      continue;
    }
    if (cur_frame.history_.empty()) {
      candidate = id;
      break;
    }
    auto cur_earliest_ts = *cur_frame.history_.begin();
    if (cur_frame.history_.size() < k_) {
      if (!seen_less_than_k || cur_earliest_ts < min_ts) {
        candidate = id;
        min_ts = cur_earliest_ts;
        seen_less_than_k = true;
      }
    } else if (!seen_less_than_k && (!seen_evictable || cur_earliest_ts < min_ts)) {
      candidate = id;
      min_ts = cur_earliest_ts;
    }
    seen_evictable = true;
  }
  BUSTUB_ASSERT(seen_evictable, "curr_size_ greater than 0 but cannot find evictable frame");
  node_store_.erase(node_store_.find(candidate));
  curr_size_--;
  *frame_id = candidate;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  PreCheck(frame_id);
  current_timestamp_++;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    //    node_store_.insert(std::make_pair(frame_id, FrameMeta{frame_id}));
    node_store_.emplace(frame_id, LRUKNode{frame_id});
    it = node_store_.find(frame_id);
  }
  //  bug: fail to update the object inside the container??
  auto &frame = it->second;
  frame.history_.emplace_back(current_timestamp_);
  if (frame.history_.size() > k_) {
    frame.history_.erase(it->second.history_.begin());
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  PreCheck(frame_id);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto &frame = iter->second;
  if (set_evictable && !frame.is_evictable_) {
    curr_size_++;
  } else if (!set_evictable && frame.is_evictable_) {
    curr_size_--;
  }
  frame.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  PreCheck(frame_id);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto &frame = iter->second;
  if (!frame.is_evictable_) {
    throw Exception("remove non evitable frame is not allowed");
  }
  node_store_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::PreCheck(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("invalid frame_id");
  }
}

}  // namespace bustub
