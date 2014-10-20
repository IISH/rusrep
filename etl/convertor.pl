#!/usr/bin/perl
#
# Copyright (C) 2014 International Institute of Social History.
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use DBI;
my %dbconfig = loadconfig("$Bin/../config/russianrep.conf");
$dir = $dbconfig{path};
$path = $dbconfig{path};
$database = $dbconfig{maindb};
$dir=~s/\s+$//g;

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

$MAX = 300;
$dir = $ARGV[0];
$filename = $ARGV[1];
$datadir = "$Bin/../datasets";
mkdir $datadir unless (-e $datadir);
$datadir.="/tmp";
mkdir $datadir unless (-e $datadir);

use File::List;

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
	   my @lines = split(/\n/, $dataset);
	   my @names = split(/\|/, $lines[0]);
	   %fields = getfields(@names);
	   %dataset = dataset_processor(@lines);

	   $table = "repository";
	   $table = "$thisfile";
	   $sql.="CREATE table $table (\n";
	   print instruction "INSERT INTO $database select * from $table;\n";
	   $sql.="\tindicator_id integer $default{integer}, \n";

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

		    if ($varname && $value)
		    {
			my $sqlvarname = $varname;
			my $sqlvalue = $value;
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
	
	        print datasets "INSERT into $table ($sqlFields) VALUES ($sqlValues);\n" if ($sqlValues=~/\w/i);
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
