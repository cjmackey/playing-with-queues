

class Job
  def initialize(opts={})
    @opts = opts.clone
  end
  def request_time
    @opts[:request_time]
  end
  def run_duration
    @opts[:run_duration]
  end
  attr_accessor :start_time
  def finish_time
    start_time + run_duration
  end
  def queue_duration
    start_time - request_time
  end
end

class Box
  def initialize(opts={})
    @opts = opts.clone
  end
  def launch_time
    @opts[:launch_time]
  end
  def launch_duration
    @opts[:launch_duration]
  end
  def usable?(time)
    time >= launch_time + launch_duration && (termination_time ? time < termination_time : true)
  end
  attr_accessor :termination_time, :job
  def available?(time)
    usable?(time) && (!job || time >= job.finish_time)
  end
end

class QueueRunner
  attr_accessor :time, :queued_jobs, :on_demand_boxes, :completed_jobs, :running_jobs
  
  def initialize(opts={})
    @opts = opts.clone
    @spot_boxes = []
    @on_demand_boxes = []
    @future_jobs = @opts[:jobs].clone.sort { |a,b| a.request_time <=> b.request_time }
    @queued_jobs = []
    @running_jobs = []
    @completed_jobs = []
    @time = 0
    (@opts[:initial_box_count] || 0).times do
      launch_on_demand(:launch_duration => 0)
    end
  end
  
  def boxes
    @on_demand_boxes + @spot_boxes
  end
  
  def usable_boxes
    boxes.find_all { |b| b.usable?(@time) }
  end
  
  def available_boxes
    boxes.find_all { |b| b.available?(@time) }
  end
  
  def on_demand_launch_duration
    @opts[:on_demand_launch_duration]
  end
  
  def spot_launch_duration
    @opts[:spot_launch_duration]
  end
  
  def new_box(opts={})
    Box.new({:launch_time => @time}.merge(opts))
  end
  
  def launch_spot(opts={})
    @spot_boxes << new_box({:launch_duration => spot_launch_duration}.merge(opts))
  end
  
  def launch_on_demand(opts={})
    @on_demand_boxes << new_box({:launch_duration => on_demand_launch_duration}.merge(opts))
  end
  
  def stop_spot
    box ||= @on_demand_boxes.shuffle.find { |b| b.available? }
    raise 'no available box exists!' unless box
    box.termination_time = @time
  end
  
  def stop_on_demand(box=nil)
    box ||= @on_demand_boxes.shuffle.find { |b| b.available? }
    raise 'no available box exists!' unless box
    box.termination_time = @time
  end
  
  def step_to_completion
    step until @future_jobs.size == 0 && @queued_jobs.size == 0 && @running_jobs.size == 0
  end
  
  def step
    @time += 1
    
    process_job_arrays
    
    if 0 == @time % @opts[:manage_job_period]
      manage_boxes
    end
    
    process_job_arrays
    
    self
  end
  
  def manage_boxes
  end
  
  def process_job_arrays
    to_enqueue = @future_jobs.take_while { |j| j.request_time <= @time }
    @future_jobs.shift(to_enqueue.size)
    @queued_jobs += to_enqueue
    
    done_jobs = @running_jobs.find_all { |j| j.finish_time <= @time }
    @running_jobs.delete_if { |j| done_jobs.include?(j) }
    @completed_jobs += done_jobs
    
    available_boxes.shuffle.each do |b|
      if @queued_jobs.size > 0
        job = @queued_jobs.shift
        b.job = job
        job.start_time = @time
        @running_jobs << job
      end
    end
  end
  
  def run(queue_entries)
    {:spots => 0, :on_demands => 0}
  end
  
end

