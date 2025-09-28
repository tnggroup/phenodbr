#rsync download of the COVID-CNS IDP files
Processed IDPs outputted from the pipeline are in /rds/project/rds-V1ceP8atvtE/Data/Proc/<sub>/IDP_files/ (Richard Bethlehem)

    ssh login-cpu.hpc.cam.ac.uk
    
In Home
    
    #old
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/*_CNS* -maxdepth 1 -iname 'IDP_files' > idp.txt
    
    #new
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/*  -maxdepth 1 -iname 'IDP_files' > idp.txt
    
    #only count unique (_2 are always? duplicates)
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/*  -maxdepth 1 -iname 'IDP_files' | grep -v '_2' | wc -l #gives me 448
    find /rds/project/rds-V1ceP8atvtE/Data/Proc/ -maxdepth 1 -type d | grep -v '_2' | wc -l #gives me 501
    
Edit the idp.txt file to remove any entries that the filter above could not deal with (i.e. the depr_ prefix).
    
    mkdir IDP_ALL
    
    awk -f filterIDPs.awk idp.txt
    
IDP files should now be filtered out in a folder per participant named with the correct study person ID, in the IDP_ALL folder

DO MANUAL EDITS OF PARTICIPANT IDS (FOLDER NAMES) BEFORE IMPORT FOR THEM TO BE MATCHED CORRECTLY WITH UPDATED IDS IN THE DATABASE! This is done automatically for numeric part mismatches by the R import routine - assumes missing initial 0s.

You will have to sort updated folders with _2 suffixes, so these are imported separately, after the first import.


#additional
Download files and folders like this (the command downloads one file and one folder - edit before running)

    rsync -avzht --progress 'login-cpu.hpc.cam.ac.uk:/home/hpczvrs1/filterIDPs.awk /home/hpczvrs1/IDP_ALL' ./
