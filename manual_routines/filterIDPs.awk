length()>8
{
	split($0,fp,"/");
	#print(fp[7]);
	if(length(fp[7])>1)
	{
		np= "/" fp[2] "/" fp[3] "/" fp[4] "/" fp[5] "/" fp[6] "/./" fp[7] "/" fp[8];
		participantId=substr(fp[7],5);
		print(participantId);
		rsyncCommand = "rsync -avzht " $0 "/ ./IDP_ALL/" participantId; #do not forget the space here!!
		print(rsyncCommand);
		system(rsyncCommand);
	}
}
