#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <memory>
#include <algorithm>
#include <set>
#include "Barrier.cpp"
#include <array>
#include <unistd.h>

/** DECLARATIONS */
struct ThreadContext;
struct JobContext;
void* thread_func(void * tc);
void map_phase(ThreadContext* tc);
void shuffle_phase(ThreadContext* job_context);
bool comp_intermid(IntermediatePair& p1, IntermediatePair& p2);
bool comp_set(K2* key1, K2* key2);
bool comp_keys(const K2* const& k1, const K2* const& k2);
void reduce_phase(ThreadContext* tc);

/** TYPEDEF */
typedef std::shared_ptr<std::atomic<uint32_t>> shared_ptr_atomic;



struct ThreadContext {
    IntermediateVec * intermid_vec;
    int thread_id;
    JobContext* job_context;
    bool terminate_flag;
};


struct JobContext {
    std::shared_ptr<std::atomic<uint32_t>> input_counter;
    std::shared_ptr<std::atomic<uint32_t>> inter_counter;
    std::shared_ptr<std::atomic<uint32_t>> shuffled_counter;
    std::shared_ptr<std::atomic<uint32_t>> output_counter;
    std::shared_ptr<std::atomic<uint64_t>> state;
    std::shared_ptr<pthread_mutex_t> job_mutex;
    std::shared_ptr<pthread_mutex_t> wait_mutex;
    Barrier* barrier;
    int job_id;
    const MapReduceClient * client;
    InputVec input_vec;
    ThreadContext* all_contexts;
    pthread_t* threads;
    int num_threads;
    std::vector<IntermediateVec>* shuffle_queue;
    OutputVec* output_vec;

};

void emit2 (K2* key, V2* value, void* context)
{
  ThreadContext * tc = static_cast<ThreadContext*>(context);
  IntermediatePair inter_pair = std::make_pair (key,value);
  tc->intermid_vec->push_back (inter_pair);
  tc->job_context->inter_counter->fetch_add (1);
}


void emit3 (K3* key, V3* value, void* context)
{
  ThreadContext * tc = static_cast<ThreadContext*>(context);
  if (pthread_mutex_lock (tc->job_context->job_mutex.get())!=0)
  {
    std::cerr << "system error: pthread lock emit3 failed" << std::endl;
    closeJobHandle (static_cast<JobHandle>(tc->job_context));
    exit (1);
  }
  OutputPair output_pair = std::make_pair (key, value);
  tc->job_context->output_vec->push_back (output_pair);
  tc->job_context->output_counter->fetch_add (1);
  tc->job_context->state->fetch_add (1);
  if(pthread_mutex_unlock (tc->job_context->job_mutex.get())!=0)
  {

    std::cerr << "system error: pthread unlock emit3 failed" << std::endl;
    closeJobHandle (static_cast<JobHandle>(tc->job_context));
    exit (1);

  }
}

void update_state(stage_t stage, JobContext* jc)
    {
      uint64_t total=0;
      uint64_t stage_converted = static_cast<uint64_t>(stage);
      if (stage_converted == 1)
      {
        total = jc->input_vec.size();
      }
      else if (stage_converted == 2)
      {
        total = jc->inter_counter->load ();
      }
      else if (stage_converted == 3)
      {
        total = jc->shuffle_queue->size();
        //total = jc->output_vec->size();
      }
      uint64_t result = (stage_converted << (62)) |
          (total << 31);

      jc->state->store (result);

    }

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  try
  {
    // memory allocations
    pthread_t * threads = new pthread_t[multiThreadLevel];
    JobContext * job_context = new JobContext;
    ThreadContext* contexts = new ThreadContext[multiThreadLevel];
    shared_ptr_atomic input_counter =
        std::make_shared<std::atomic<uint32_t>>(0);
    std::vector<IntermediateVec>* shuffle_queue = new
        std::vector<IntermediateVec>;
    shared_ptr_atomic inter_counter = std::make_shared<std::atomic<uint32_t>>
        (0);
    shared_ptr_atomic output_counter = std::make_shared<std::atomic<uint32_t>>
        (0);
    shared_ptr_atomic ended_map_thread =
        std::make_shared<std::atomic<uint32_t>>
        (0);
    shared_ptr_atomic shuffled_counter =
        std::make_shared<std::atomic<uint32_t>>
            (0);
    std::shared_ptr<std::atomic<uint64_t>> state =
        std::make_shared<std::atomic<uint64_t>>
            (0);
    std::shared_ptr<JobState> job_state = std::make_shared<JobState>();
    std::shared_ptr<pthread_mutex_t> job_mutex
    =std::make_shared<pthread_mutex_t>();
    std::shared_ptr<pthread_mutex_t> wait_mutex
        =std::make_shared<pthread_mutex_t>();
    Barrier * barrier = new Barrier(multiThreadLevel);


    // update job_context
    job_context->state = state;
    job_context -> input_counter = input_counter;
    job_context -> inter_counter = inter_counter;
    job_context -> output_counter = output_counter;
    job_context -> shuffled_counter = shuffled_counter;
    job_context -> client = &client;
    job_context -> input_vec = inputVec;
    job_context -> barrier = barrier;
    job_context-> all_contexts = contexts;
    job_context-> num_threads = multiThreadLevel;
    job_context->shuffle_queue = shuffle_queue;
    job_context->job_mutex = job_mutex;
    job_context->wait_mutex = wait_mutex;
    job_context->output_vec = &outputVec;
    job_context->threads = threads;

    // update threads contexts
    for (int i = 0; i < multiThreadLevel; ++i) {
      contexts[i].job_context = job_context;
      contexts[i].thread_id = i;
      IntermediateVec* intermediate_vec = new IntermediateVec;
      contexts[i].intermid_vec = intermediate_vec;
      contexts[i].terminate_flag = false;

    }
    // create threads
    update_state (MAP_STAGE, job_context);
    JobHandle job_handle = static_cast<JobHandle>(job_context);
    for (int i = 0; i < multiThreadLevel; ++i) {
      if (pthread_create(threads+i, nullptr, thread_func, contexts+i)!= 0)
      {
        std::cerr << "system error: pthread_create failed" << std::endl;
        closeJobHandle (job_handle);
        exit (1);
      }

    }

    return job_handle;
  }

  catch (std::bad_alloc &ba)
  {
    std::cerr << "system error: memory allocation failed" << std::endl;
    exit(1);
  }
}

void map_phase(ThreadContext* tc){
  int vec_size = tc->job_context->input_vec.size();
  while (true)
  {
    uint32_t old_input_counter = tc->job_context->input_counter->fetch_add (1);
    if (old_input_counter >= vec_size)
    {
      break;
    }
    InputPair input_pair = tc->job_context->input_vec[old_input_counter];
    tc->job_context->client->map (input_pair.first, input_pair.second, tc);
    tc->job_context->state->fetch_add (1);
  }
  std::sort(tc->intermid_vec->begin(), tc->intermid_vec->end(), comp_intermid);
}

void reduce_phase(ThreadContext* tc){
  int vec_size = tc->job_context->shuffle_queue->size();
  while (true)
  {
    uint32_t old_shuffled_counter = tc->job_context->shuffled_counter->fetch_add
        (1);
    if (old_shuffled_counter >= vec_size)
    {
      break;
    }
    IntermediateVec* inter_vec = &(tc->job_context->shuffle_queue->at
        (old_shuffled_counter));
    tc->job_context->client->reduce(inter_vec, tc);
//    tc->job_context->state->fetch_add (1);
  }
}


bool comp_intermid(IntermediatePair& p1, IntermediatePair& p2)
{
  return p1.first->operator< (*p2.first);
}

void shuffle_phase(ThreadContext* tc)
{
  std::set<K2 *, bool(*)(const K2* const& k1, const K2* const& k2)> keys(comp_keys);
  for (int i = 0; i < tc->job_context->num_threads; ++i)
  {
    if (tc->job_context->all_contexts[i].intermid_vec->empty()) {
      continue;
    }
    for (int j = 0; j < tc->job_context->all_contexts[i].intermid_vec->size
        (); j++)
    {
      keys.insert ((tc->job_context->all_contexts[i].intermid_vec)->at (j).first);
    }
  }
  for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
    K2 *curr_key = *it;
    IntermediateVec key_vec;
    for(int i=0; i < tc->job_context->num_threads; ++i) {
      if (tc->job_context->all_contexts[i].intermid_vec->empty()) {
        continue;
      }
      IntermediatePair last_pair =
          tc->job_context->all_contexts[i].intermid_vec->back();
      K2 *last_key = last_pair.first;
      while ( !(tc->job_context->all_contexts[i].intermid_vec->empty())&&
      (!comp_keys(last_key, curr_key) && !comp_keys (curr_key, last_key)))
      {
        key_vec.push_back (last_pair);
        tc->job_context->state->fetch_add (1);
        tc->job_context->all_contexts[i].intermid_vec->pop_back ();
        last_pair = tc->job_context->all_contexts[i].intermid_vec->back();
        last_key = last_pair.first;
      }
    }
    tc->job_context->shuffle_queue->push_back (key_vec);
  }

}

bool comp_keys(const K2* const& k1, const K2* const& k2) {
  return *k1 < *k2;
}

void* thread_func(void* thread_context)
{
  ThreadContext * tc = static_cast<ThreadContext *>(thread_context);
  // map phase
  map_phase (tc);
  tc->job_context->barrier->barrier();

  if (tc->thread_id == 0)
  {
    update_state (SHUFFLE_STAGE, tc->job_context);
    shuffle_phase (tc);
    update_state (REDUCE_STAGE, tc->job_context);

  }
  tc->job_context->barrier->barrier();
  reduce_phase (tc);


  return thread_context;
}


void waitForJob(JobHandle job)
{
  JobContext * job_context = static_cast<JobContext*>(job);
  if (pthread_mutex_lock (job_context->wait_mutex.get())!=0)
  {
    std::cerr << "system error: pthread lock in wait failed" << std::endl;
    closeJobHandle (job);
    exit (1);
  }

  for (int i=0; i<job_context->num_threads; i++)
  {
    if (!(job_context->all_contexts[i].terminate_flag))
    {
      if (pthread_join (job_context->threads[i], NULL)!=0)
      {
        std::cerr << "system error: pthread join failed" << std::endl;
        closeJobHandle (job);
        exit (1);
      }
      job_context->all_contexts[i].terminate_flag = true;
    }
  }

  if (pthread_mutex_unlock (job_context->wait_mutex.get())!=0)
  {
    std::cerr << "system error: pthread unlock in wait failed" << std::endl;
    closeJobHandle (job);
    exit (1);
  }
}

void getJobState(JobHandle job, JobState* state)
{
  JobContext * job_context = static_cast<JobContext*>(job);
  uint64_t all_state = job_context->state->load ();
  state->stage = static_cast<stage_t>(all_state >> (62));
  uint64_t mask_cur = 2147483647;
  uint64_t cur_count = mask_cur&all_state;
  uint64_t mask_total = 2147483647;
  uint64_t total = mask_total&(all_state>>31);
  float perc = (cur_count/(float)total)*100;
  state->percentage = perc;
}

void closeJobHandle(JobHandle job)
{
  JobContext *job_context = static_cast<JobContext *>(job);
  waitForJob (job);
  delete[] job_context->all_contexts;
  delete job_context->barrier;
  delete[] job_context->threads;
  delete job_context->shuffle_queue;
  if (pthread_mutex_destroy (job_context->job_mutex.get ()) != 0)
  {
    std::cerr << "system error: pthread job_mutex dest failed" << std::endl;
    exit (1);
  }
  if (pthread_mutex_destroy (job_context->wait_mutex.get ()) != 0)
  {
    std::cerr << "system error: pthread wait_mutex dest failed" << std::endl;
    exit (1);
  }
  delete job_context;
}