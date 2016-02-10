import os

docker_id = "0ede86ece3ce"
f = open("filenames_1.txt", "r")
for filename in f:
    filename = filename.rstrip()
    analysis_id = filename.replace(".fastq.tar.gz", "")
    filepath = "/home/ubuntu/SCRATCH/%s" %(filename)
    print analysis_id
    output = "/home/ubuntu/SCRATCH/geuvadis_results/%s" %(analysis_id)
    if not os.path.isdir(output):
	os.mkdir(output)

    os.system("docker run -v /home/ubuntu/:/host/home -v /etc:/host/etc -v /mnt/SCRATCH:/home/ubuntu/SCRATCH/ -i -t %s /usr/bin/python /home/ubuntu/expression/pipeline_elastic_cluster_new.py --analysis_id %s --gtf /home/ubuntu/SCRATCH/geuvadis_genome/gencode.v19.annotation.hs37d5_chr.gtf --input_file %s --p 8 --star_pipeline /home/ubuntu/expression/icgc_rnaseq_align/star_align.py --output_dir %s --genome_fasta_file /home/ubuntu/SCRATCH/geuvadis_genome/hs37d5.fa  --genome_dir /home/ubuntu/SCRATCH/geuvadis_genome/star_genome/ --quantMode TranscriptomeSAM --cufflinks_pipeline /home/ubuntu/expression/compute_expression.py --ref_flat /home/ubuntu/SCRATCH/geuvadis_genome/refFlat.txt" %(docker_id, analysis_id, filepath, output))






