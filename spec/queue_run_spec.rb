$LOAD_PATH << '.'

require 'queue_run'

describe QueueRunner do
  let(:jobs) { [] }
  let(:t0) { 0 }
  let(:initial_box_count) { 0 }
  let(:manage_job_period) { 50*60 }
  let(:box_manager) { nil }
  let(:default_q_opts) { ({:jobs => jobs,
                            :manage_job_period => manage_job_period,
                            :box_manager => box_manager,
                            :on_demand_launch_duration => 7*60,
                            :spot_launch_duration => 11*60,
                            :t0 => t0,
                            :initial_box_count => initial_box_count,
                            :reserved_count => 0,
                            :reserved_price => 1.0,
                            :on_demand_price => 2.1,
                            :spot_price => 0.35,
                          }) }
  let(:q_opts) { default_q_opts }
  let(:q) { QueueRunner.new(q_opts) }
  it('starts with time zero') { q.time.should == 0 }
  it('steps to time one') { q.step(1).time.should == 1 }
  it('steps to the next job management time') { q.step.time.should == 50*60 }
  it('calculates cost with one box') { q.launch_on_demand.step(1).costs.should == 2.1 }
  context "with a different t0" do
    let(:t0) { 15 }
    it("starts at a different time") { q.time.should == 15 }
  end
  context "with some starting boxes" do
    let(:initial_box_count) { 3 }
    it("starts with boxes") { q.boxes.size.should == 3 }
    let(:jobs) { [Job.new(:request_time => 0, :run_duration => 10)] }
    it("can start jobs immediately") { q.step(0).running_jobs.size.should == jobs.size }
    it("can terminate one") { q.terminate_on_demand.usable_boxes.size.should == 2 }
  end
  context "with a single job" do
    let(:jobs) { [Job.new(:request_time => 7*60, :run_duration => 30)] }
    it('can step with nothing happening') { q.step(1).queued_jobs.should == [] }
    it('can step to something') { q.step.queued_jobs.should == jobs }
    it('launches box') do
      q.launch_on_demand
      q.usable_boxes.size.should == 0
      q.on_demand_boxes.size.should == 1
      (7*60).times { q.step }
      q.usable_boxes.size.should == 1
    end
    it('runs a job') do
      q.step(1)
      q.launch_on_demand
      q.step
      q.queued_jobs.size.should == 1
      q.step
      q.queued_jobs.size.should == 0
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      q.step(29)
      q.completed_jobs.size.should == 0
      q.step(1)
      q.completed_jobs.size.should == 1
    end
    it("runs to completion") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 7*60 + 30
    end
  end
  context "with two jobs, one box" do
    let(:jobs) { [Job.new(:request_time => 7*60, :run_duration => 30),
                  Job.new(:request_time => 7*60, :run_duration => 40)] }
    it("runs 'em") do
      q.step(1)
      q.launch_on_demand
      q.step
      q.queued_jobs.size.should == 2
      q.step
      q.queued_jobs.size.should == 1
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      q.step(29)
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      q.step(1)
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 1
      q.step
      q.completed_jobs.size.should == 2
    end
    it("runs to completion") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 7*60+30+40
      q.steps.should == 3
    end
    it("has stats") do
      q.launch_on_demand
      q.step_to_completion
      q.completed_jobs.map { |j| j.queue_duration } == [0,40]
    end
  end
  context "with many jobs, one box" do
    let(:jobs) { 50.times.to_a.map { Job.new(:request_time => 7*60, :run_duration => 30) } }
    it("has stats") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 50*30+7*60
      q.completed_jobs[0].queue_duration.should == 0
      q.completed_jobs[1].queue_duration.should == 30
      q.completed_jobs[2].queue_duration.should == 60
      (((q.completed_jobs.map { |j| j.queue_duration }).inject(:+))/50.0).should == ((50-1)/2.0)*30
      q.percentile_queue_duration(0).should == 0
      q.percentile_queue_duration(2).should == 30
      q.percentile_queue_duration(99).should == 30*49
      q.percentile_queue_duration(90).should == 30*45
    end
  end
  context "when running with some default autostart box management" do
    let(:initial_box_count) { 10 }
    let(:manage_job_period) { 5*60 }
    let(:jobs) { 1000.times.to_a.map { |i| Job.new(:request_time => i*5+3*60, :run_duration => 60) } }
    let(:box_manager) do
      lambda do |q|
        q.samples ||= []
        q.samples << { :idlers => q.available_boxes.size }
        if q.time % (5*60) == 0
          depth = q.queued_jobs.size
          idle_workers = q.available_boxes.size
          want = ((depth / 3) - idle_workers.size).to_i # Idle workers are assumed to grab items in the queue shortly
          want = 1 if idle_workers.size == 0 and want <= 0
          
          min_historic_idle_hosts = (q.samples[-5..-1] || [{:idlers => 1}]).map { |s| s[:idlers] }.min
          
          if want > 0
            want.times do
              q.launch_on_demand
            end
          elsif min_historic_idle_hosts > 1
            q.terminate_on_demand
          end
        end
      end
    end
    it('runs...') do
      q.step_to_completion
      puts q.time
      puts q.steps
      puts q.boxes.size
      puts q.usable_boxes.size
      puts q.percentile_queue_duration(50)
      puts q.percentile_queue_duration(60)
      puts q.percentile_queue_duration(70)
      puts q.percentile_queue_duration(80)
      puts q.percentile_queue_duration(90)
    end
  end
end
