#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use DBI;
my %dbconfig = loadconfig("/etc/apache2/russianrep.config");
$dir = $dbconfig{path};
$path = $dbconfig{path};
$database = $dbconfig{maindb};
$tmpdir = $dbconfig{tmppath};
$dir=~s/\s+$//g;

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

$MAX = 300;
$dir = $ARGV[0];
$filename = $ARGV[1];
$datadir = "$Bin/../newdatasets";
mkdir $datadir unless (-e $datadir);
$datadir.="/tmp";

use File::List;
unless ($tmpdir)
{
    $datadir = "$Bin/../datasets";
}
else
{
    $tmpdir=~s/\/$//g;
    $datadir = $tmpdir."/../datasets";
}
mkdir $datadir unless (-e $datadir);

my $search = new File::List($dir);
my @files  = @{ $search->find("\.xls") };    # find all perl scripts in /usr/local

$integer = "integer";
$float = "double precision";
$varchar = "character varying(512)";
%default = ($integer, "DEFAULT 0", $float, "DEFAULT 0", $varchar, "");

my $sql;

open(instruction, ">$datadir/upload.sql");
foreach $file (@files)
{
   my $true = 0;
   unlink $file if ($file=~/\.sql/);
   $true = 1 if (!$filename || $file=~/$filename/i);

   if ($true)
   {
        print "$file\n";
        @folders = getdirlist($file);
        ($thisdir, $thisfile) = makefolders($datadir, @folders);
	$thisfile=~s/\)|\(//g;
	open(datasets, ">$datadir/$thisfile.sql");
	open(datasetinfo, ">$datadir/$thisfile.dump");
        extractor($file, $thisfile, "$thisdir/$thisfile");
	close(datasets);
	close(datasetinfo);
   }
};
close(instruction);

sub extractor
{
   my ($file, $thisfile, $outfile, $DEBUG) = @_;

   my $sql;
   if (-e $file && $file=~/\.xls/ && $file!~/\.sql/) # && $file=~/Nabor\_age\_1897\_finalAM/i)
#   if ($file=~/Nabor\_age\_1897\_finalAM/i)
   # && $file=~/\)/)
   {
	my $newfile = $file;
	my $baseyear;
	if ($newfile=~/(\d+)\.xls/)
	{
	    $baseyear = $1;
	}

	$newfile=~s/\(\d+\)//g;
	$newfile=~s/\s+//g;
	if ($file ne $newfile)
	{
	    $mv = `mv \"$file\" $newfile`;
	    $file = $newfile;
	};

	my $dataset = `$Bin/xlsx2csv.py --delimiter=\"|\" $file`;
	if ($dataset=~/\w+/sxi)
	{
	   my (%public, %varnames);
	   $thisfile=~s/\.\w+$//g;
	   print "$thisfile\n";
	   open(outfile, ">$outfile.lex");
	   open(dataset, ">$outfile.data");
	   print dataset "$dataset";
	   close(dataset);
	   my @lines;
	   my @slines = split(/\n/, $dataset);
	   $ID = 0;
	   foreach $item (@slines)
	   {
		unless ($item=~/^\"/)
		{
		    push(@lines, $item);
		    $ID++;
	 	}
		else
		{
		    $pID = $ID - 1;
		    $lines[$pID].="$item";
		}
	   }
	   # Add baseyear
	   @lines[0] = "base_year|$lines[0]";
	   for ($i=1; $i<=$#lines; $i++)
	   {
	       $lines[$i] = "$baseyear|$lines[$i]";
	   }
	   my @names = split(/\|/, $lines[0]);
	   %fields = getfields(@names);
	   %dataset = dataset_processor(@lines);

	   $table = "repository";
	   $table = "$thisfile";
	   $sql.="CREATE table $table (\n";
	   print instruction "INSERT INTO $database select * from $table;\n";
	   $sql.="\tindicator_id integer $default{integer}, \n";
	   #$sql.="\tbase_year $varchar $default{$varchar}, \n";

	   foreach $id (sort {$order{$a} <=> $order{$b}} keys %order)
	   {
	        my $thisfield = $fields{$id};
		$field = $thisfield;
		my $vartype;
	        my $values;
	        #if ($dataset{$id}{unique} < 10)

	        #if ($field!~/value$/i && $dataset{$id}{unique} < $MAX)
		$field=~s/\-/\_/g;
		$field=~s/\s+/\_/g;
		#$field=~s/Comment\_naborschik$/Comment\_naborschika/gsxi;
	 	if ($field=~/^(\S)(.+)$/)
		{
		    $field = uc($1).$2;
		}
                if ($field=~/^(\S+\_)(\S)(.+)$/)
                {
                    $field = $1.uc($2).$3;
                }

		$public{$thisfield}{name} = $field;
		$public{$thisfield}{id} = $order{$thisfield} || '0';
		$varnames{$id} = $field;

		if ($field)
	        {
		   ($values, $vartype) = vocabulary($id, 0, $dataset{$id}{count});
	        }
	        print outfile "\#$id <$field> $vartype $dataset{$id}{unique}\n$values\n";
		$sql.="\t$field $vartype $default{$vartype},\n" if ($field=~/\w+/);
	   }
	   close(outfile);

	   my ($sqlFields, $sqlValues);
	   for ($i=1; $i<=$#lines; $i++)
	   {
		@values = split(/\|/, $lines[$i]);
		my $sqlValues;
		my $sqlFields;
		$values[1000] = $table;
		for ($fID=0; $fID<=$#values; $fID++)
		{
		    my ($varname, $value) = ($varnames{$fID}, $values[$fID]);
		    $value=~s/^\s*\.\s*$//g;

		    if ($varname && $value=~/\S+/)
		    {
			my $sqlvarname = $varname;
			my $sqlvalue = $value;
			if ($sqlvarname=~/datatype/i)
			{
			    $sqlvalue=~s/\.$//g;
			    $sqlvalue = sprintf("%.02f", $sqlvalue);
			}
			if ($sqlvarname=~/code/i)
			{
			    $sqlvalue=~s/\"|\'//g;
			}
		
			if ($i eq 1 && $sqlvarname!~/Comment\_Naborshika/i)
			{
                	   $sqlvarname=~s/\-/\_/g;
                	   $sqlvarname=~s/\s+/\_/g;
			   $fields{$fID} = "$sqlvarname";
			}

			if ($sqlvalue=~/^\.$/)
			{
			    $sqlValues.="NULL, ";
			}
			else
			{
		            $sqlValues.=$dbh->quote("$sqlvalue").", ";
			    $sqlFields.="$fields{$fID}, ";
			}
		        print "$fID $varnames{$fID} $values[$fID]\n" if ($DEBUG);
	 	    }
		}
		$sqlValues=~s/\r|\n//g;
		$sqlValues=~s/\,\s+$//g;
		$sqlFields=~s/\,\s+$//g;
	
		$maintable = "russianrepository";
	        print datasets "INSERT into $maintable ($sqlFields) VALUES ($sqlValues);\n" if ($sqlValues=~/\w/i);
	   };
	}

        $sql=~s/\,$//;
        $sql.=");\n";
        print datasetinfo "$sql\n\n";
   };

   return;
}

sub getfields
{
   my (@names) = @_;
   my (%fieldnames, $lastID);
 
   for ($i=0; $i<$#names; $i++)
   {
	print "$i $names[$i]\n" if ($DEBUG);
	$fieldnames{$i} = $names[$i];
	$order{$i} = $i;
	$lastID = $i;
   }

   $lastID = 999;
   $fieldnames{$lastID} = 'base_year';
   $order{$lastID} = $lastID;
   $lastID = 1000;
   $fieldnames{$lastID} = 'indicator';
   $order{$lastID} = $lastID;

   return %fieldnames;
}

sub dataset_processor
{
   my (@lines) = @_;
   my %dataset;

   for ($i=1; $i<$#lines; $i++)
   {
	my $item = $lines[$i];
	$item=~s/^\s+|\s+$//g;
	my @subitems = split(/\|/, $item);

	for ($j=0; $j<$#subitems; $j++)
	{
	   my $subitem = $subitems[$j];
	   $dataset{$j}{unique}++ unless ($dataset{$j}{count}{$subitem});
	   $dataset{$j}{count}{$subitem}++;
	   #print "[$i][$j]$fields{$j} $subitem\n";
	};
	#print "@items\n";
   }

   return %dataset;
}

sub vocabulary
{
    my ($id, $fieldname, $valhash, $DEBUG) = @_;
    my (%values, $output, $type, $vartype, %types);

    %values = %{$valhash} if ($valhash);
    foreach $value (sort keys %values)
    {
	$output.="\t$value\n ";
	$type = $varchar; # if ($value=~/\d+\.\d+/);
	$type = $integer if ($value=~/^\d+$/);
	$type = $varchar if ($value=~/\D+/ && $value!~/^\d+$/);
	$type = $float if ($value=~/\d+\.\d+/ && $value!~/\s+\-/);
	$type = $varchar;
	$types{$type}++;
	#print "$value $type\n";
    } 

    foreach $type (sort {$types{$b} <=> $types{$a}} keys %types)
    {
	$vartype = $type;
	break;
    }

    $vartype = $varchar if ($types{$varchar}>10);
    $vartype = $float if ($types{$float});
       
    unless ($vartype)
    {
	$vartype = $varchar;
    }

    return ($output, $vartype);
}

sub getdirlist
{
    my ($file) = @_;

    $file=~s/^\W+//;
    @folders = split(/\//, $file);

    return @folders;
}

sub makefolders
{
    my ($dir, @folders) = @_;
    my $path;
 
    $datafile = pop @folders;
    foreach $subdir (@folders)
    {
	$path.="/$subdir";	
	mkdir ("$dir$path") unless (-e "$dir$path");
	print "$dir$path\n" if ($DEBUG);
    }

    return ("$dir$path", $datafile);
}

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;
        my ($name, $value) = split(/\s*\=\s*/, $str);
        $config{$name} = $value;
    }
    close(conf);

    return %config;
}
