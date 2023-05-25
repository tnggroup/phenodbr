#rsync download of the COVID-CNS IDP files
Processed IDPs outputted from the pipeline are in /rds/project/rds-V1ceP8atvtE/Data/Proc/<sub>/IDP_files/ (Richard Bethlehem)

    ssh login-cpu.hpc.cam.ac.uk
    
In Home
    
    #old
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/RGZ_* -maxdepth 1 -iname 'IDP_files' > idp.txt
    
    #new
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/*_CNS* -maxdepth 1 -iname 'IDP_files' > idp.txt
    
Edit the idp.txt file to remove any entries that the filter above could not deal with (i.e. the depr_ prefix).
    
    mkdir IDP_ALL
    
    awk -f filterIDPs.awk idp.txt
    
IDP files should now be filtered out in a folder per participant named with the correct study person ID, in the IDP_ALL folder

DO MANUAL EDITS OF PARTICIPANT IDS (FOLDER NAMES) BEFORE IMPORT FOR THEM TO BE MATCHED CORRECTLY WITH UPDATED IDS IN THE DATABASE! This is done automatically for numeric part mismatches by the R import routine - assumes missing initial 0s.


#additional
Download files and folders like this

    rsync -avzht --progress 'login-cpu.hpc.cam.ac.uk:/home/hpczvrs1/filterIDPs.awk /home/hpczvrs1/IDP_ALL' ./
