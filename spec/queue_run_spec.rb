$LOAD_PATH << '.'

require 'queue_run'

describe QueueRunner do
  let(:jobs) { [] }
  let(:default_q_opts) { ({:jobs => jobs,
                            :manage_job_period => 5*60,
                            :on_demand_launch_duration => 7*60,
                            :spot_launch_duration => 11*60,
                          }) }
  let(:q_opts) { default_q_opts }
  let(:q) { QueueRunner.new(q_opts) }
  it('starts with time zero') { q.time.should == 0 }
  it('steps to time one') { q.step.time.should == 1 }
  context "a single job" do
    let(:jobs) { [Job.new(:request_time => 7*60, :run_duration => 30)] }
    it('can step with nothing happening') { q.step.queued_jobs.should == [] }
    it('can step to something') do
      (7*60).times { q.step }
      q.queued_jobs.should == jobs
    end
    it('launches box') do
      q.launch_on_demand
      q.usable_boxes.size.should == 0
      q.on_demand_boxes.size.should == 1
      (7*60).times { q.step }
      q.usable_boxes.size.should == 1
    end
    it('runs a job') do
      q.step
      q.launch_on_demand
      (7*60-1).times { q.step }
      q.queued_jobs.size.should == 1
      q.step
      q.queued_jobs.size.should == 0
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      29.times { q.step }
      q.completed_jobs.size.should == 0
      q.step
      q.completed_jobs.size.should == 1
    end
    it("runs to completion") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 7*60 + 30
    end
  end
  context "two jobs" do
    let(:jobs) { [Job.new(:request_time => 7*60, :run_duration => 30),
                  Job.new(:request_time => 7*60, :run_duration => 30)] }
    it("runs 'em") do
      q.step
      q.launch_on_demand
      (7*60-1).times { q.step }
      q.queued_jobs.size.should == 2
      q.step
      q.queued_jobs.size.should == 1
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      29.times { q.step }
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 0
      q.step
      q.running_jobs.size.should == 1
      q.completed_jobs.size.should == 1
      30.times { q.step }
      q.completed_jobs.size.should == 2
    end
    it("runs to completion") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 7*60+2*30
    end
    it("has stats") do
      q.launch_on_demand
      q.step_to_completion
      q.completed_jobs.map { |j| j.queue_duration } == [0,30]
    end
  end
  context "many jobs!" do
    let(:jobs) { 50.times.to_a.map { Job.new(:request_time => 7*60, :run_duration => 30) } }
    it("has stats") do
      q.launch_on_demand
      q.step_to_completion
      q.time.should == 50*30+7*60
      q.completed_jobs[0].queue_duration.should == 0
      q.completed_jobs[1].queue_duration.should == 30
      q.completed_jobs[2].queue_duration.should == 60
      (((q.completed_jobs.map { |j| j.queue_duration }).inject(:+))/50.0).should == ((50-1)/2.0)*30
    end
  end
end
