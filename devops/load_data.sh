data_dir=$1
db_name=${2:-"lake"}

for path in $(ls $data_dir/*.csv)
do
    filename=$(python -c "print('$path'.split('.csv')[0].split('/')[-1])")
    #echo "Generated schema (from head)"
    #echo $filename
    #head $path -n 100 | csvsql -i postgresql
    echo "Inserting into table $filename given path $path"
    csvsql --db postgresql://admin:admin@localhost:5432/$db_name --tables $filename --insert $path --no-create --no-inference
done
