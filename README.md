# Creation of CSV file from JSON file downloaded from GH Archive

Once Dockerfile is started, the script contained allows downloading a JSON file from GH Archive automatically. 
After that it goes to extracts the information in the JSON file and if a commit message it is equal to a word contained 
inside a list, the script go to add it into a CSV file. 

The execution of those phases is multiprocess then you can have multiple download, JSON file analysis and CSV creation
at the same time.

After the download, you can find the JSON file into the specific directory called "gh_archive_zip_files".

After the CSV creation, you can find it into the specific directory called "gh_archive_csv"

Vigimella - 2022.