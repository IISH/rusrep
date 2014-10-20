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
use lib "$libpath/../libs";

use DB_File;
use DBI;
use lib '/home/clio-infra/cgi-bin/libs';
use lib './libs';
use ClioInfra;
use ClioTemplates;
use Ord;
$| = 1;

#$site = "http://node-149.dev.socialhistoryservices.org";
$scriptdir = "/home/clio-infra/cgi-bin";
$countrylink = "datasets/countries";
$indicatorlink ="indicator.html";

$htmltemplate = "$Bin/../templates/countries.tpl";
#@html = loadhtml($htmltemplate);

my %dbconfig = loadconfig("$scriptdir/config/russianrep.config");
$site = $dbconfig{root};
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{webdbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh_web = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

#print "Content-type: text/html\n\n";

#print "Regions\n";
#$datatype = '7.01';
$sqlquery = "select distinct datatype from russianrepository where 1=1 ";
$sqlquery.=" and datatype='$datatype'" if ($datatype);
if ($sqlquery)
{
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($datatype) = $sth->fetchrow_array())
    {
	print "$datatype\n";
        import_class($datatype);
    }
}
#import_class("1.01");
#import_class(2);

sub import_class
{
   my ($datatype, $class, $class_prev, $class_name, $DEBUG) = @_;
   my %h;

   $sqlquery = "select histclass1, histclass2, histclass3, datatype from russianrepository where datatype = '$datatype'"; # and histclass1 IS NOT NULL limit 10";
   print "SQL $sqlquery\n";

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($histclass1, $histclass2, $histclass3, $datatype) = $sth->fetchrow_array())
    {
	if ($datatype=~/^(\d+)/ && $histclass1)
	{
	    my $topic = $1;
#	    print "#1$histclass1 #2$histclass2 #3$histclass3 =>$datatype $topic\n";
	    my $index = ordering($histclass1);
	    $h{$histclass1} = $topic;
	    $topics{$topic} = $index;
	    $index{$histclass1} = $index;

	    my $index = ordering($histclass2);
	    $levels{$histclass1}{$histclass2} = $index if ($histclass2);

            my $index = ordering($histclass3);
            $levels{$histclass1}{$histclass2}{$histclass3} = $index if ($histclass3);
	    
	    $next = $class+1;
#	    import_class($next, $class, "$histclass") if ($next <= 3); 
	    
#	    $dbh->do("insert into datasets.histclasses (region_name, region_code, active, region_description, region_ord) values ('$region', '$ter_code', 1, '$region', '$index')");
	    $known{$ter_code}++;
        }
   }

   foreach $histclass1 (sort keys %h)
   {
        $orig_datatype = $datatype;
	if ($datatype!~/\-/)
	{
	   $datatype = sprintf("%.2f", $datatype);
	};
	print "$datatype', '$histclass1\n";

	$dbh->do("insert into datasets.histclasses (datatype, histclass1, histclass2, histclass3, histclass_root, topic, index, level) values ('$datatype', '$histclass1', '0', '0', '0', '$h{$histclass1}', '$index{$histclass1}', '1')");

	my %histclasses = %{$levels{$histclass1}};
	foreach $histclass2 (sort keys %histclasses)
	{
	     if ($datatype=~/^(\d+)/)
	     {
		$topic = $1;
	     }
	     $dbh->do("insert into datasets.histclasses (datatype, histclass1, histclass2, histclass3, histclass_root, topic, index, level) values ('$datatype', '$histclass1', '$histclass2', '0', '$topic', '$h{$histclass1}', '$index{$histclass1}', '2')");

	     my %level3 = %{$histclasses{$histclass2}};
	     foreach $histclass3 (sort keys %level3)
	     {
		if ($datatype=~/^(\d+)/)
		{
		    $topic = $1;
		}
		$dbh->do("insert into datasets.histclasses (datatype, histclass1, histclass2, histclass3, histclass_root, topic, index, level) values ('$datatype', '$histclass1', '$histclass2', '$histclass3', '$h{$histclass1}', '$topic', '$index{$histclass1}', '3')");
	     }
	}
   }
};
