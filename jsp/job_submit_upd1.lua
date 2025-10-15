-- job_submit.lua
function slurm_job_submit(job_desc, part_list, submit_uid)

    --PURPOSE: detect unspecified mail user and substitute with username@example.com
    if job_desc.mail_user == nil then
        job_desc.mail_user = job_desc.user_name .. "@example.edu"
    end

    return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
    return slurm.SUCCESS
end
