-- job_submit.lua
function slurm_job_submit(job_desc, part_list, submit_uid)

    -- PURPOSE: requests for all memory engages --exclusive
    if (job_desc.min_mem_per_node == 0) then
      job_desc.shared = 2
    end

    return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
    return slurm.SUCCESS
end
