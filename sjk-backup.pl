#!/usr/bin/perl
# Copyright (c) 2011 Svante J. Kvarnstrom <sjk@ankeborg.nu>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

use strict;
use warnings;

use Config::Scoped;
use File::Basename;
use File::Copy;
use File::Path qw(remove_tree);
use File::Rsync;
use Getopt::Std;
use Parallel::ForkManager;
use POSIX qw(strftime);
use sigtrap qw(handler cleanup_and_exit normal-signals);

use constant {
	CONFFILE	=> '/usr/local/etc/sjk-backup.conf',
};

################################################################################
#### GLOBAL VARIABLES
################################################################################

my $VERSION = '0.1';

# Reference to config hash
my $config;

# Reference to hash containing command line options used
my $args;

# Verbosity level (log_level in config)
my $verbose;

# Directory containing lock/pid files.
my $lock_directory;


################################################################################
#### SJK-BACKUP
################################################################################

$args = parse_args();

VERSION_MESSAGE() if defined $args->{'V'};
HELP_MESSAGE() if defined $args->{'h'};

# Read the configuration file.
$config = read_conf();

# Run various checks on config.
check_config();

$verbose = $config->{'general'}{'log_level'};
$lock_directory = strip_trailing_slash($config->{'general'}{'lock_directory'});

# Do the backups.
do_backups($config);


################################################################################
#### FUNCTIONS
################################################################################

# Tell everyone what version we're running!
sub VERSION_MESSAGE {
	my $prog = basename($0);
	print "$prog version $VERSION\n";
	exit(1);
}

sub HELP_MESSAGE {
	my $prog = basename($0);
	print "$prog version $VERSION\n\n";
	print "-h, --help: This help text\n";
	print "-v, --version: display what version of $prog we are running\n";
	print "-m <machine>: Machine to backup. Must be listed in sjk-backup.conf\n";
	exit(1);
}

# Parse command line arguments.
sub parse_args {
	my %args;
	my $res;

	if (($res = getopts('hvVm:', \%args)) != 1) {
		print STDERR "Type $0 --help for help.\n";
		exit(1);
	}

	return \%args;
}


# Rotate backups (and delete oldest)
sub rotate_backups {
	my $name = shift;
	my $max = $config->{'backup'}{$name}{'number_of_backups'};
	my $backup_root = $config->{'general'}{'backup_root'}."/".$name;
	my $backup = $backup_root."/".$name;
	
	for (my $i = $max; $i != -1; $i--) {
		my $j = $i + 1;

		if (-d "$backup.$i" && $i == $max) {
			write_log("Removing $backup.$i.", 5);
			remove_tree("$backup.$i", { verbose => 0 }) or die "Could not remove_tree $backup.$i: $!";
			next;
		}

		if (-d "$backup.$i") {
			write_log("Moving $backup.$i to $backup.$j", 5);
			move("$backup.$i", "$backup.$j") or die "Could not move $backup.$i to $backup.$j: $!";
		}
	}

}

# Creates lock file. Prints warning and returns 1 if file already exists.
# If the file doesn't exist it creates it and returns 0.
sub create_lock_file {
	my $name = shift;
	my $file = $lock_directory."/".$name.".lock";
	
	if (-e $file) {
		write_log("Error creating lock file: $file does already exist!", 1);
		return 1;
	}

	write_log("Creating lockfile $file", 5);

	open my $fh, ">$file" or die "Could not create $file: $!";
	close $fh;

	return 0;
}

# Removes lock file. Prints warning and returns 1 if it does not succeed, 
# returns 0 otherwise.
sub remove_lock_file {
	my $name = shift;
	my $file = $lock_directory."/".$name.".lock";

	write_log("Removing lockfile $file", 5);

	if (unlink($file) == 0) {
		write_log("Error removing lock file ($file): $!", 1);
		return 1;
	}

	return 0;
}

# Write pid to lock file.
sub write_pid {
	my ($name, $pid) = @_;
	
	my $file = $lock_directory."/".$name.".lock";
	if (!-e $file) {
		write_log("Error writing pid to file: $file does not exist.", 1);
		return 1;
	}

	write_log("Writing pid ($pid) to $file", 5);
	
	open my $fh, ">$file" or die "Could not open $file for writing: $!";
	print $fh $pid;
	close $fh;

	return 0;
}

# Run checks on config.
sub check_config {
	check_backup_root($config->{'general'}->{'backup_root'});
	check_lock_directory($config->{'general'}->{'lock_directory'});
}

# Returns a reference to a structure holding the config data.
sub read_conf {
	my $parser = new Config::Scoped file => CONFFILE;
	$config = $parser->parse;
	return $config;
}

# Check if backup_root is defined, if it exists and is writable.
sub check_backup_root {
	my $root = shift;
	die "No backup_root defined" unless defined $config->{'general'}{'backup_root'};
	die "$root does not exist" unless -e $root;
	die "$root is not writable by us" unless -w $root;
	die "$root is not a directory" unless -d $root;
}

# Check if lock_directory is defined, if it exists and is writable.
sub check_lock_directory {
	my $directory = shift;

	die "No lock_directory defined" unless defined $config->{'general'}{'lock_directory'};
	die "$directory does not exist. Please create it." unless -d $directory;
	die "$directory is not writable" unless -w $directory;
}

# Prints warning if $level is <= $verbose (defined in config as log_level).
sub print_warning {
	my ($message, $level) = @_;
	
	if (!defined($verbose) || ($level <= $verbose)) {
		print STDERR "Warning: $message\n";
	}
}

# Print info if $level is <= $verbose (defined in config as log_level.
sub print_info {
	my ($message, $level) = @_;

	if (!defined($verbose) || ($level <= $verbose)) {
		print STDERR "Info: $message\n";
	}
}

# Iterate through the hosts that should be backed up and execute the parallel
# rsyncs.
sub do_backups {
	my $config = shift;
	my $pfm = new Parallel::ForkManager($config->{'general'}{'max_concurrent_rsyncs'}); 

	# If we have been given orders about backing up a specific machine, using
	# the -m switch, we should only worry about that.
	if (defined($args->{'m'})) {
		die "$args->{'m'} is not in the config file." unless defined $config->{'backup'}{$args->{'m'}};
		die "Could not create lock file - are we already rsyncing this?" if create_lock_file($args->{'m'});
		backup_host($args->{'m'}, $config);
		remove_lock_file($args->{'m'});
		return;
	}
		
	# Otherwise we would backup all the machines listed in sjk-backup.conf.
	foreach my $key (keys %{$config->{'backup'}}) {
		# If the lock file already exists a sync is probably already going.
		next if create_lock_file($key); 

		my $pid = $pfm->start and next; # Fork!

		# Write the pid to the lock file.
		die if write_pid($key, $pid);
		backup_host($key, $config);

		remove_lock_file($key);
		$pfm->finish;
	}

	$pfm->wait_all_children;
}

sub backup_host {
	my ($name,$conf) = @_;
	my $hostconf = $conf->{'backup'}->{$name};

	my $user = $hostconf->{'user'};
	my $host = $hostconf->{'host'};
	my $backup_root = $conf->{'general'}->{'backup_root'};
	my $bwlimit = $hostconf->{'bwlimit'} ? $hostconf->{'bwlimit'} : 0;
	my $exclude = $hostconf->{'exclude'};

	my $dst = "$backup_root/$name/$name.".mkdate().".unfinished";
	#my $dst = "$backup_root/$name/$name.0.unfinished";
	#my $prev = "$backup_root/$name/$name.0";
	my $prev = readlink "$backup_root/$name/$name.latest";
	chomp($prev);

	# XXX: Don't do this until the backup has finished! Otherwise we'll be in
	# trouble if the user cancels the backup and restarts it.
	# Update the <name>.latest symlink.
	#write_log("Unlinking $name.latest", 5);
	#unlink("$backup_root/$name/$name.latest") or die "Could not unlink $backup_root/$name/$name.latest: $!";

	#write_log("Creating $name.latest -> $dst link", 5);
	#symlink($dst, "$backup_root/$name/$name.latest") or die "Could not create $backup_root/$name/$name.latest -> $dst link: $!";

	my $secs = $config->{'general'}{'seconds_between_retries'};
	my $retries = $config->{'general'}{'retries'};

	my %settings = (
		#'acls'				=> 1,
		'archive'			=> 1,
		'delete'			=> 1,
		'delete-excluded'	=> 1,
		'hard-links'		=> 1,
		'human-readable'	=> 1,
		'inplace'			=> 1,
		'link-dest'			=> [ $prev ],
		'exclude'			=> $exclude,
		'numeric-ids'		=> 1,
		'one-file-system'	=> 1,
		'partial'			=> 1,
		'relative'			=> 1,
	);

	$settings{'bwlimit'} = $bwlimit if defined $bwlimit;
	#$settings{'link-dest'} = $prev if -d $prev;

	is_directory("$backup_root/$name") or mkdir("$backup_root/$name");

	foreach my $path (@{$hostconf->{'path'}}) {
		$path = strip_trailing_slash($path);
		my $src;

		if (defined($user) && defined($host)) {
			$src = "$user\@$host:\"$path\"";
		} else {
			$src = $path;
		}

		write_log("Backing up $src", 2);

		my $rsync = File::Rsync->new(\%settings);

		for (my $i = 0; $i <= $retries; $i++) {
			my $ret = $rsync->exec({ src => $src, dest => $dst});

			write_log("Try $i: ".$rsync->lastcmd, 3);

			# If rsync succeeds, break the loop.
			last if $ret;

			# We failed. Tell the user about it, wait and try again.
			write_log("Rsync of $src to $dst failed, waiting $secs seconds before trying again.", 1);
			write_log("Rsync exited with status ".$rsync->status, 5);
			write_log("Rsync command used: ".$rsync->lastcmd, 5);

			sleep $secs;
		}

	}

	#write_log("Rotating backups", 5);
	#rotate_backups($name);
	
	#write_log("Moving $dst to $backup_root/$name.0", 5);
	#move("$dst", "$backup_root/$name/$name.0") or die "Could not move $dst to $backup_root/$name.0: $!";

	# Now that the backup has finished we should rename the backup and update
	# the <name>.latest link.
	my $newdst = $dst;
	$newdst =~ s,\.unfinished,,g;
	write_log("Moving $dst to $newdst", 5);
	move($dst, $newdst) or die "Could not move $dst to $newdst: $!";

	write_log("Unlinking $name.latest", 5);
	unlink("$backup_root/$name/$name.latest") or die "Could not unlink $backup_root/$name/$name.latest: $!";

	write_log("Creating $name.latest -> $newdst link", 5);
	symlink($newdst, "$backup_root/$name/$name.latest") or die "Could not create $backup_root/$name/$name.latest -> $newdst link: $!";
}

# is_comment($line)
# Returns true if $line is a comment, and false if it is not.
sub is_comment {
	my $line = shift;
	$line =~ s/^(?:\s|\t)+//;
	if ($line =~ /^#/) {
		return 1;
	} else {
		return 0;
	}
}

# is_empty_line($line)
# Returns true if $line is empty, false if it isn't
sub is_empty_line {
	my $line = shift;
	if ($line =~ /^(?:\s|\t)*$/) {
		return 1;
	} else {
		return 0;
	}
}

sub is_directory {
	my $dir = shift;
	return 1 if -d $dir;

	return 0;
}

sub strip_trailing_slash {
	my $line = shift;

	# Don't do anything if $line is a local or remote root filesystem.
	return $line if ($line eq '/');
	return $line if ($line =~ m,:/$,);

	# Otherwise strip trailing slash.
	$line =~ s,/+$,,;

	return $line;
}

# Return date to use in log files
sub mklogdate {
	return localtime;
}

sub mkdate {
	return strftime('%Y-%m-%d.%H-%M-%S', localtime);
#	my ($sec, $min, $hour, $mday, $mon, $year, undef) = localtime(time);
#	$year += 1900;
#	return "$year-$mon-$mday.$hour-$min-$sec";
}

# Write log message.
sub write_log {
	my ($line, $level) = @_;

	my $logfile = $config->{'general'}{'log_file'};

	if (!defined($verbose) || ($level > $verbose)) {
		return (undef);
	}

	chomp($line);

	open my $fh, ">> $logfile" or die "Could not open $logfile: $!";
	my $date = mklogdate();

	print $fh "$date: $line\n";
	
	print "$date: $line\n" if $args->{'v'};

	close $fh or die "Could not close $logfile: $!";
}

# This function is called when program is killed.
sub cleanup_and_exit {
	# Clear out the lock file directory.
	remove_tree($lock_directory, { verbose => 0, keep_root => 1 }) or die "Could not remove_tree $lock_directory: $!";
}
