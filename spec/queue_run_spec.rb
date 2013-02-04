$LOAD_PATH << '.'

require 'queue_run'

describe QueueRunner do
  let(:jobs) { [] }
  let(:default_q_opts) { ({:jobs => jobs,
                            :manage_job_period => 50*60,
                            :on_demand_launch_duration => 7*60,
                            :spot_launch_duration => 11*60,
                          }) }
  let(:q_opts) { default_q_opts }
  let(:q) { QueueRunner.new(q_opts) }
  it('starts with time zero') { q.time.should == 0 }
  it('steps to time one') { q.step(1).time.should == 1 }
  it('steps to the next job management time') { q.step.time.should == 50*60 }
  context "with a different t0" do
    let(:q_opts) { default_q_opts.merge(:t0 => 15) }
    it("starts at a different time") { q.time.should == 15 }
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
end
